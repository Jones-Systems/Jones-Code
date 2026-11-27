// @effect-diagnostics nodeBuiltinImport:off -- Fixtures own only their unique synthetic homes.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";
import { describe, it, expect, vi } from "vite-plus/test";
import { EnvironmentId, type JonesUpdateState } from "@t3tools/contracts";
import type {
  JonesBoundOperationStartInput,
  JonesOperationReceipt,
  JonesOperationStartInput,
} from "@t3tools/contracts/jones/jonesOperations";
import type {
  JonesActionsCandidate,
  JonesActionsCheckResult,
} from "@t3tools/shared/jones/jonesActions";
import { JonesUpdater, type JonesPreparedStage, type JonesUpdaterHost } from "../JonesUpdater.ts";
import type { OperationReconciliation } from "../launcherOperation.ts";
import {
  makePreparationSupervisor,
  type PreparationJoin,
  type PreparationSupervisor,
} from "../preparation/supervisor.ts";
import { preparationLifecycle } from "../preparation/lifecycle.ts";
import { preparationJobsDirectory } from "../preparation/protocol.ts";
import { sweepPreparationLeftovers } from "../preparation/sweep.ts";
import { sourceWorkerCommand } from "../preparation/Preparation.testkit.ts";
import { withQualifiedRuntimeLock } from "../../cloud/qualifiedRuntime.ts";
import { createFleetHostStore, type FleetFollowerRecord } from "../../fleetUpdates/store.ts";
import { preparationJobLock } from "../preparation/protocol.ts";
import { createOperationLedger } from "./ledger.ts";
import { createTargetOperations } from "./operations.ts";
import { createRecoveryPreflights } from "./recoveryPreflight.ts";

vi.mock("node:fs/promises", async (importOriginal) => ({
  ...(await importOriginal<typeof import("node:fs/promises")>()),
}));

function deferred<A>() {
  let resolve!: (value: A) => void;
  let reject!: (cause: unknown) => void;
  const promise = new Promise<A>((yes, no) => {
    resolve = yes;
    reject = no;
  });
  return { promise, resolve, reject };
}
const input: JonesOperationStartInput = {
  operationId: "11111111-1111-4111-8111-111111111111",
  environmentId: EnvironmentId.make("target-host"),
  expectedInstalledSource: "a".repeat(40),
  action: { kind: "download-exact", targetSource: "b".repeat(40) },
};
const candidate: JonesActionsCandidate = {
  schema: 1,
  repository: "Jones-Systems/Jones-Code",
  source: "b".repeat(40),
  tree: "c".repeat(40),
  installedSource: "a".repeat(40),
  workflow: ".github/workflows/artifact-cli-linux.yml",
  workflowId: 1,
  runId: 2,
  runAttempt: 1,
  ciRunId: 3,
  artifactId: 4,
  artifactName: "fixture",
  artifactDigest: `sha256:${"d".repeat(64)}`,
  artifactBytes: 1,
  expiresAt: "2099-01-01T00:00:00Z",
  version: "0.0.0-preview.20261002.2.1",
  platform: "linux",
  architecture: "x64",
};
const enrollment = {
  enrollmentId: "22222222-2222-4222-8222-222222222222",
  environmentId: input.environmentId,
  enabled: true,
  continueRunningThreads: false,
};
const directive = {
  enrollmentId: enrollment.enrollmentId,
  directiveId: "33333333-3333-4333-8333-333333333333",
  targetSource: candidate.source,
  targetTree: candidate.tree,
  mode: "manual-review",
  issuedAt: "2026-11-19T00:00:00Z",
  leaderAtIssue: null,
} as const;
const follower: FleetFollowerRecord = {
  schema: 1,
  leader: null,
  operation: null,
  lastAutomaticActivationAt: null,
  directive,
};
const legacyPlan = {
  ...input,
  correlation: {
    planId: "44444444-4444-4444-8444-444444444444",
    deviceKey: input.environmentId,
  },
};
const ordinaryPlan: JonesBoundOperationStartInput = {
  ...legacyPlan,
  admission: { kind: "ordinary" },
};
const followerPlan: JonesBoundOperationStartInput = {
  ...legacyPlan,
  admission: {
    kind: "follower",
    enrollmentId: directive.enrollmentId,
    directiveId: directive.directiveId,
    targetSource: directive.targetSource,
    targetTree: directive.targetTree,
  },
};
async function fixture(
  run: (f: Awaited<ReturnType<typeof make>>) => Promise<void>,
  recoveryEnabled = false,
  hostInstall?: JonesUpdaterHost["install"],
) {
  const base = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-target-operation-"));
  try {
    await run(await make(base, recoveryEnabled, hostInstall));
  } finally {
    await NodeFSP.rm(base, { recursive: true, force: true });
  }
}
async function make(
  base: string,
  recoveryEnabled: boolean,
  hostInstall?: JonesUpdaterHost["install"],
  supervisor?: PreparationSupervisor,
) {
  const joins = new Map<string, PreparationJoin | "running">();
  let removedJobIds: string[] = [];
  const preparation: PreparationSupervisor = supervisor ?? {
    start: () => {
      throw new Error("This test injects the external preparation boundary through host.prepare.");
    },
    run: async () => {
      throw new Error("No unrelated worker admitted.");
    },
    inspect: (id) => joins.get(id) ?? "unknown",
    sweep: async () => ({
      kind: "sweep",
      removed: removedJobIds.length,
      retained: 0,
      failed: 0,
      removedJobIds,
    }),
  };
  const outcome = deferred<JonesPreparedStage>();
  const join = deferred<PreparationJoin>();
  let jobId: string | undefined;
  let prepareCalls = 0;
  let checkBarrier: Promise<void> | undefined;
  let reservationBarrier: Promise<void> | undefined;
  let publicationBarrier: Promise<void> | undefined;
  const checkEntered = deferred<void>();
  const admissionReserved = deferred<void>();
  const prepareStarted = deferred<void>();
  const reservationEntered = deferred<void>();
  const receipts = new Map<string, JonesOperationReceipt>();
  const receiptWaiters = new Set<{
    phase: JonesOperationReceipt["phase"];
    resolve: (receipt: JonesOperationReceipt) => void;
  }>();
  const check = vi.fn(async (): Promise<JonesActionsCheckResult> => {
    checkEntered.resolve();
    await checkBarrier;
    return { state: "available", candidate };
  });
  const updater = new JonesUpdater(
    {
      initialState: {
        source: "jones-actions",
        channel: "jones-main",
        phase: "available",
        environmentId: input.environmentId,
        currentVersion: "0.0.0-preview.20261002.1",
        installedSource: {
          repository: "Jones-Systems/Jones-Code",
          sha: input.expectedInstalledSource,
          tree: "e".repeat(40),
        },
        capability: {
          check: true,
          download: true,
          install: true,
          exactSource: true,
          operations: 1,
          boundAdmission: 1,
          ...(recoveryEnabled ? { recoveryPolicy: 1 as const } : {}),
        },
      },
      platform: "linux",
      architecture: "x64",
      stateChanged: (state) => {
        const r = state.operation;
        if (r === undefined) return;
        receipts.set(r.operationId, r);
        for (const waiter of receiptWaiters) {
          if (r.operationId === input.operationId && r.phase === waiter.phase) {
            receiptWaiters.delete(waiter);
            waiter.resolve(r);
          }
        }
      },
      installedSource: async () => input.expectedInstalledSource,
      prepare: async (_candidate, options) => {
        jobId = NodeCrypto.randomUUID();
        reservationEntered.resolve();
        await reservationBarrier;
        await options.onReserved?.(jobId);
        prepareCalls++;
        // The receipt is durable before an external worker can start.
        expect((await createOperationLedger(base).read(input.operationId))?.jobId).toBe(jobId);
        joins.set(jobId, "running");
        const settled = join.promise.then((j) => {
          joins.set(j.jobId, j);
          return j;
        });
        options.onStarted?.(settled);
        prepareStarted.resolve();
        const abort = () => outcome.reject(options.signal.reason);
        options.signal.addEventListener("abort", abort, { once: true });
        try {
          const stage = await outcome.promise;
          await settled;
          options.signal.throwIfAborted();
          await options.onAdopting?.();
          await publicationBarrier;
          return stage;
        } finally {
          options.signal.removeEventListener("abort", abort);
        }
      },
      install: hostInstall ?? (async () => {}),
    },
    { check },
  );
  const reserve = updater.reserveCheckAdmission.bind(updater);
  vi.spyOn(updater, "reserveCheckAdmission").mockImplementation(() => {
    const reservation = reserve();
    admissionReserved.resolve();
    return reservation;
  });
  const ledger = createOperationLedger(base);
  const fleetStore = createFleetHostStore(base);
  let native: OperationReconciliation = { state: "absent", operationId: input.operationId };
  const install = vi.fn(
    async (
      _request: Parameters<Parameters<typeof createTargetOperations>[0]["install"]>[0],
    ): Promise<JonesUpdateState> => ({ ...updater.snapshot(), phase: "installing" }),
  );
  let recoveryTime = Date.parse("2026-10-27T04:00:00Z");
  const recovery = createRecoveryPreflights({
    snapshot: () => updater.snapshot(),
    nativeSupported: () => recoveryEnabled,
    now: () => recoveryTime,
    measure: async () => ({
      firstInstall: false,
      sameVolume: true,
      bytes: 100,
      availableBytes: 100,
      copyRequiredBytes: 200,
      cloneRequiredBytes: 10,
      clone: { status: "proved" },
    }),
  });
  const refreshPolicy = vi.fn(async () => {});
  const reconcileNative = vi.fn(async (_operationId: string) => native);
  const resolveNativeHandoff = vi.fn(
    async (_id: string): Promise<"accepted" | "rejected" | "unknown"> => "unknown",
  );
  const operations = createTargetOperations({
    key: base,
    ledger,
    fleetStore,
    updater,
    preparation,
    refreshPolicy,
    install,
    reconcileNative,
    resolveNativeHandoff,
    onFailure: () => {},
    ...(recoveryEnabled
      ? { recovery: { validate: recovery.validate, observed: async () => "clone" as const } }
      : {}),
  });
  const receipt = () => ledger.read(input.operationId);
  return {
    base,
    ledger,
    fleetStore,
    operations,
    resolveNativeHandoff,
    preparation,
    updater,
    install,
    receipt,
    check,
    checkEntered: checkEntered.promise,
    admissionReserved: admissionReserved.promise,
    prepareStarted: prepareStarted.promise,
    reservationEntered: reservationEntered.promise,
    refreshPolicy,
    reconcileNative,
    receiptAt: (phase: JonesOperationReceipt["phase"]) => {
      const current = receipts.get(input.operationId);
      return current?.phase === phase
        ? Promise.resolve(current)
        : new Promise<JonesOperationReceipt>((resolve) => receiptWaiters.add({ phase, resolve }));
    },
    recovery,
    expireRecovery: () => {
      recoveryTime += 300_000;
    },
    prepareCalls: () => prepareCalls,
    jobId: () => jobId,
    blockCheck: (promise: Promise<void>) => {
      checkBarrier = promise;
    },
    blockReservation: (promise: Promise<void>) => {
      reservationBarrier = promise;
    },
    blockPublication: (promise: Promise<void>) => {
      publicationBarrier = promise;
    },
    setNative: (value: OperationReconciliation) => {
      native = value;
    },
    finish: (kind: PreparationJoin["kind"]) => {
      const id = jobId!;
      join.resolve({ kind, jobId: id });
      if (kind === "completed")
        outcome.resolve({
          stagedHandle: "stage",
          version: candidate.version!,
          payloadSha256: "f".repeat(64),
        });
      else outcome.reject(new Error("External worker ended."));
    },
    scratchDeleted: () => {
      removedJobIds = [jobId!];
    },
    release: () => {
      joins.set(jobId!, { kind: "stopped", jobId: jobId! });
    },
  };
}
describe("plan-bound target admission", () => {
  it("durably refuses ordinary admission after enrollment wins during discovery", async () =>
    fixture(async (f) => {
      const peer = createFleetHostStore(f.base);
      const releaseCheck = deferred<void>();
      f.blockCheck(releaseCheck.promise);
      const discovery = f.updater.check();
      await f.checkEntered;
      const started = f.operations.start(ordinaryPlan);
      await f.admissionReserved;
      try {
        await peer.editEnrollment(() => enrollment);
      } finally {
        releaseCheck.resolve();
      }
      await discovery;
      const refused = await started;
      expect(refused).toMatchObject({
        ...ordinaryPlan,
        code: "admission-route-changed",
        phase: "refused",
      });
      expect(refused.acceptedAt).toBeUndefined();
      expect(await f.receipt()).toEqual(refused);
      await peer.editEnrollment(() => ({ ...enrollment, enabled: false }));
      expect(await f.operations.start(ordinaryPlan)).toEqual(refused);
      expect(await f.operations.start(followerPlan)).toMatchObject({ code: "operation-conflict" });
      await expect(f.operations.fence(followerPlan)).rejects.toThrow("original request");
      expect(await f.receipt()).toEqual(refused);
      expect(f.prepareCalls()).toBe(0);
      expect(f.install).not.toHaveBeenCalled();
    }));

  it.each(["disabled enrollment", "same-source directive"] as const)(
    "refuses a follower when the competing %s write wins the fleet lock",
    async (change) =>
      fixture(async (f) => {
        const peer = createFleetHostStore(f.base);
        await peer.editEnrollment(() => enrollment);
        await peer.editFollower(() => follower);
        const destination = NodePath.join(
          f.base,
          "runtime",
          "jones-fleet",
          change === "disabled enrollment" ? "enrollment.json" : "follower.json",
        );
        const entered = deferred<void>();
        const release = deferred<void>();
        const originalRename = NodeFSP.rename;
        const rename = vi.spyOn(NodeFSP, "rename").mockImplementation(async (from, to) => {
          if (to === destination) {
            entered.resolve();
            await release.promise;
          }
          await originalRename(from, to);
        });
        const edit =
          change === "disabled enrollment"
            ? peer.editEnrollment(() => ({ ...enrollment, enabled: false }))
            : peer.transactDirective((old) => ({
                record: {
                  ...old!,
                  directive: { ...directive, directiveId: NodeCrypto.randomUUID() },
                },
                result: undefined,
              }));
        try {
          await entered.promise;
          const prepared = deferred<void>();
          f.refreshPolicy.mockImplementation(async () => prepared.resolve());
          const started = f.operations.start(followerPlan);
          await prepared.promise;
          expect(await f.receipt()).toBeNull();
          release.resolve();
          await edit;
          const refused = await started;
          expect(refused).toMatchObject({
            ...followerPlan,
            phase: "refused",
            code: "admission-route-changed",
          });
          expect(refused.acceptedAt).toBeUndefined();
          expect(await f.receipt()).toEqual(refused);
          expect(f.prepareCalls()).toBe(0);
          expect(f.install).not.toHaveBeenCalled();
        } finally {
          release.resolve();
          await edit;
          rename.mockRestore();
        }
      }),
  );

  it("commits exact follower acceptance before a waiting enrollment edit", async () =>
    fixture(async (f) => {
      const peer = createFleetHostStore(f.base);
      await peer.editEnrollment(() => enrollment);
      await peer.editFollower(() => follower);
      const entered = deferred<void>();
      const release = deferred<void>();
      const accept = f.ledger.accept.bind(f.ledger);
      let committed = false;
      const spy = vi.spyOn(f.ledger, "accept").mockImplementation(async (request) => {
        entered.resolve();
        await release.promise;
        const result = await accept(request);
        committed = result.created;
        return result;
      });
      const started = f.operations.start(followerPlan);
      try {
        await entered.promise;
        await expect(
          withQualifiedRuntimeLock(f.base, "jones-fleet", async () => undefined),
        ).rejects.toThrow("already owns this home");
        const edit = peer.editEnrollment(() => {
          expect(committed).toBe(true);
          return { ...enrollment, enabled: false };
        });
        release.resolve();
        const accepted = await started;
        expect(accepted).toMatchObject({
          ...followerPlan,
          phase: "accepted",
          acceptedAt: expect.any(String),
        });
        await edit;
        await f.prepareStarted;
        f.finish("completed");
        const completed = await f.receiptAt("completed");
        expect(completed.admission).toEqual(followerPlan.admission);
        expect(await f.operations.start(followerPlan)).toEqual(completed);
        expect(await f.operations.fence(followerPlan)).toEqual(completed);
        expect(f.prepareCalls()).toBe(1);
      } finally {
        release.resolve();
        spy.mockRestore();
      }
    }));

  it("preserves never-enrolled V1 acceptance and refuses new V1 plan IDs even after disablement", async () =>
    fixture(async (f) => {
      expect(await f.operations.start(legacyPlan)).toMatchObject({
        ...legacyPlan,
        phase: "accepted",
      });
      await f.prepareStarted;
      f.finish("completed");
      const completed = await f.receiptAt("completed");
      const peer = createFleetHostStore(f.base);
      await peer.editEnrollment(() => ({ ...enrollment, enabled: false }));
      const other = { ...legacyPlan, operationId: NodeCrypto.randomUUID() };
      const refused = await f.operations.start(other);
      expect(refused).toMatchObject({
        ...other,
        phase: "refused",
        code: "admission-binding-required",
      });
      expect(await f.ledger.read(other.operationId)).toEqual(refused);
      expect(await f.operations.start(legacyPlan)).toEqual(completed);
      expect(await f.operations.start(ordinaryPlan)).toMatchObject({ code: "operation-conflict" });
      expect(f.prepareCalls()).toBe(1);
    }));

  it("keeps unreadable enrollment unconfirmed and preserves an exact retained receipt", async () =>
    fixture(async (f) => {
      const old = (await f.ledger.refuse(legacyPlan, "busy", "Already decided.")).receipt;
      await f.fleetStore.readEnrollment();
      await NodeFSP.writeFile(
        NodePath.join(f.base, "runtime", "jones-fleet", "enrollment.json"),
        "{}",
        { mode: 0o600 },
      );
      const newStart = { ...legacyPlan, operationId: NodeCrypto.randomUUID() };
      await expect(f.operations.start(newStart)).rejects.toThrow("Fleet receipt is invalid");
      expect(await f.ledger.read(newStart.operationId)).toBeNull();
      expect(await f.operations.start(legacyPlan)).toEqual(old);
      expect(f.prepareCalls()).toBe(0);
    }));

  it("checks the follower action source and the exact staged install identity", async () =>
    fixture(async (f) => {
      await f.fleetStore.editEnrollment(() => enrollment);
      await f.fleetStore.editFollower(() => follower);
      await f.updater.check();
      const snapshot = f.updater.snapshot.bind(f.updater);
      vi.spyOn(f.updater, "snapshot").mockImplementation(() => ({
        ...snapshot(),
        stagedHandle: "actual-stage",
      }));
      for (const action of [
        { kind: "download-exact", targetSource: "f".repeat(40) },
        {
          kind: "install",
          targetSource: candidate.source,
          currentVersion: snapshot().currentVersion!,
          stagedHandle: "another-stage",
        },
      ] as const) {
        const request = { ...followerPlan, operationId: NodeCrypto.randomUUID(), action };
        const refused = await f.operations.start(request);
        expect(refused).toMatchObject({
          ...request,
          phase: "refused",
          code: "admission-route-changed",
        });
        expect(await f.ledger.read(request.operationId)).toEqual(refused);
      }
      expect(f.prepareCalls()).toBe(0);
      expect(f.install).not.toHaveBeenCalled();
    }));

  it("keeps the original V2 fence when it wins against a pending Start and changed payload", async () =>
    fixture(async (f) => {
      const release = deferred<void>();
      f.blockCheck(release.promise);
      const discovery = f.updater.check();
      await f.checkEntered;
      const started = f.operations.start(ordinaryPlan);
      await f.admissionReserved;
      try {
        const fence = await f.operations.fence(ordinaryPlan);
        expect(fence).toMatchObject({ ...ordinaryPlan, phase: "fenced" });
        expect(fence.acceptedAt).toBeUndefined();
        release.resolve();
        expect(await started).toEqual(fence);
        expect(await f.operations.start(followerPlan)).toEqual(fence);
        expect(await f.operations.fence(followerPlan)).toEqual(fence);
        expect(await f.receipt()).toEqual(fence);
        expect(f.prepareCalls()).toBe(0);
      } finally {
        release.resolve();
        await discovery;
      }
    }));

  it.each(["fenced", "running"] as const)(
    "does not dispatch a deferred Start while another writer's %s rename is unconfirmed",
    async (phase) =>
      fixture(async (f) => {
        const request: JonesBoundOperationStartInput =
          phase === "fenced"
            ? ordinaryPlan
            : {
                ...ordinaryPlan,
                action: {
                  kind: "install",
                  targetSource: candidate.source,
                  currentVersion: "old",
                  stagedHandle: "retained-stage",
                },
              };
        const release = deferred<void>();
        f.blockCheck(release.promise);
        const discovery = f.updater.check();
        await f.checkEntered;
        const started = f.operations.start(request);
        const failure = new Error("Synthetic operation directory sync failure");
        const denied = expect(started).rejects.toThrow(failure);
        await f.admissionReserved;
        const writer = createOperationLedger(f.base);
        if (phase === "running") await writer.accept(request);
        const directory = NodePath.join(f.base, "runtime", "jones-operations");
        const path = NodePath.join(directory, `${request.operationId}.json`);
        const originalRename = NodeFSP.rename;
        const originalOpen = NodeFSP.open;
        let unconfirmed = false;
        const rename = vi.spyOn(NodeFSP, "rename").mockImplementation(async (from, to) => {
          await originalRename(from, to);
          if (to === path) unconfirmed = true;
        });
        const open = vi.spyOn(NodeFSP, "open").mockImplementation(async (...args) => {
          const handle = await originalOpen(...args);
          if (args[0] === directory && unconfirmed)
            handle.sync = async () => {
              throw failure;
            };
          return handle;
        });
        try {
          await expect(
            phase === "fenced"
              ? writer.fence(request)
              : writer.transition(request.operationId, (old) => ({ ...old, phase })),
          ).rejects.toThrow(failure);
          expect(JSON.parse(await NodeFSP.readFile(path, "utf8"))).toMatchObject({
            ...request,
            phase,
          });
          release.resolve();
          await denied;
          expect(f.updater.snapshot().operation).toBeUndefined();
          expect(f.prepareCalls()).toBe(0);
          expect(f.install).not.toHaveBeenCalled();
          unconfirmed = false;
          const confirmed = await createOperationLedger(f.base).read(request.operationId);
          expect(confirmed).toMatchObject({ ...request, phase });
          expect(await f.operations.start(request)).toEqual(confirmed);
          expect(f.prepareCalls()).toBe(0);
          expect(f.install).not.toHaveBeenCalled();
        } finally {
          release.resolve();
          open.mockRestore();
          rename.mockRestore();
          await discovery;
        }
      }),
  );
});

describe("target operations", () => {
  it.each(["download", "download-exact"] as const)(
    "joins an in-flight check before accepting %s and replays concurrent IDs with one preparation",
    async (kind) =>
      fixture(async (f) => {
        const request: JonesOperationStartInput = {
          ...input,
          action:
            kind === "download"
              ? { kind, artifactId: candidate.artifactId, sourceSha: candidate.source }
              : input.action,
        };
        const check = deferred<void>();
        f.blockCheck(check.promise);
        const background = f.updater.check();
        await f.checkEntered;
        const started = f.operations.start(request);
        await f.admissionReserved;
        const duplicate = f.operations.start(request);
        expect(await f.receipt()).toBeNull();
        expect(f.refreshPolicy).not.toHaveBeenCalled();
        expect(f.prepareCalls()).toBe(0);
        check.resolve();
        await background;
        expect(await started).toMatchObject({ phase: "accepted", operationId: input.operationId });
        expect((await duplicate).operationId).toBe(input.operationId);
        await f.prepareStarted;
        expect(f.prepareCalls()).toBe(1);
        expect(f.check).toHaveBeenCalledTimes(kind === "download" ? 1 : 2);
        f.finish("completed");
        await f.receiptAt("completed");
        expect((await f.operations.start(request)).phase).toBe("completed");
        expect(f.prepareCalls()).toBe(1);
      }),
  );

  it("keeps background checks out throughout asynchronous policy refresh and ledger acceptance", async () =>
    fixture(async (f) => {
      const policyEntered = deferred<void>();
      const policy = deferred<void>();
      const ledgerEntered = deferred<void>();
      const ledger = deferred<void>();
      f.refreshPolicy.mockImplementation(async () => {
        policyEntered.resolve();
        await policy.promise;
      });
      const accept = f.ledger.accept.bind(f.ledger);
      vi.spyOn(f.ledger, "accept").mockImplementation(async (request) => {
        ledgerEntered.resolve();
        await ledger.promise;
        return accept(request);
      });
      const started = f.operations.start(input);
      await policyEntered.promise;
      await f.updater.check();
      expect(f.check).not.toHaveBeenCalled();
      policy.resolve();
      await ledgerEntered.promise;
      await f.updater.check();
      expect(f.check).not.toHaveBeenCalled();
      ledger.resolve();
      expect((await started).phase).toBe("accepted");
      await f.prepareStarted;
      expect(f.check).toHaveBeenCalledOnce();
      f.finish("completed");
      await f.receiptAt("completed");
    }));

  it("allows a pending admission to be fenced while the background check is still in flight", async () =>
    fixture(async (f) => {
      const check = deferred<void>();
      f.blockCheck(check.promise);
      const background = f.updater.check();
      await f.checkEntered;
      const started = f.operations.start(input);
      await f.admissionReserved;
      expect((await f.operations.fence(input)).phase).toBe("fenced");
      check.resolve();
      await background;
      const fenced = await started;
      expect(fenced.phase).toBe("fenced");
      expect(fenced.acceptedAt).toBeUndefined();
      expect(f.prepareCalls()).toBe(0);
      expect(f.refreshPolicy).not.toHaveBeenCalled();
      await f.updater.check();
      expect(f.check).toHaveBeenCalledTimes(2);
    }));

  it.each([
    ["environment", "stale-binding"],
    ["installed source", "stale-binding"],
    ["operations", "operation-admission-disabled"],
    ["download", "action-unsupported"],
    ["check", "exact-source-unsupported"],
    ["exact source", "exact-source-unsupported"],
    ["artifact", "stale-binding"],
    ["stage", "another-stage"],
    ["policy", "follower-policy"],
  ])("revalidates %s after joining the check and refreshing policy", async (changed, code) =>
    fixture(async (f) => {
      const check = deferred<void>();
      f.blockCheck(check.promise);
      const background = f.updater.check();
      await f.checkEntered;
      const request: JonesOperationStartInput =
        changed === "artifact"
          ? {
              ...input,
              action: {
                kind: "download",
                artifactId: candidate.artifactId,
                sourceSha: candidate.source,
              },
            }
          : input;
      const started = f.operations.start(request);
      await f.admissionReserved;
      f.refreshPolicy.mockImplementation(async () => {
        if (changed === "policy") {
          f.updater.setPolicy({
            role: "follower",
            leaderSource: "f".repeat(40),
            leaderTree: "e".repeat(40),
          });
          return;
        }
        const snapshot = f.updater.snapshot.bind(f.updater);
        const state = snapshot();
        const { operations: _operations, ...withoutOperations } = state.capability;
        const patch: Partial<JonesUpdateState> =
          changed === "environment"
            ? { environmentId: EnvironmentId.make("another-host") }
            : changed === "installed source"
              ? { installedSource: { ...state.installedSource!, sha: "f".repeat(40) } }
              : changed === "stage"
                ? {
                    stagedHandle: "another-stage",
                    provenance: { ...state.provenance!, sourceSha: "f".repeat(40) },
                  }
                : changed === "artifact"
                  ? { provenance: { ...state.provenance!, artifactId: candidate.artifactId + 1 } }
                  : {
                      capability: {
                        ...(changed === "operations" ? withoutOperations : state.capability),
                        ...(changed === "download" ? { download: false } : {}),
                        ...(changed === "check" ? { check: false } : {}),
                        ...(changed === "exact source" ? { exactSource: false } : {}),
                      },
                    };
        vi.spyOn(f.updater, "snapshot").mockImplementation(() => ({ ...snapshot(), ...patch }));
      });
      check.resolve();
      await background;
      const refused = await started;
      expect(refused).toMatchObject({ phase: "refused", code });
      expect(refused.acceptedAt).toBeUndefined();
      expect(f.prepareCalls()).toBe(0);
      expect((await f.operations.start(request)).code).toBe(code);
      expect(f.refreshPolicy).toHaveBeenCalledOnce();
    }),
  );

  it.each(["held", "preparing", "installing"] as const)(
    "does not join discovery or accept a request while %s admission is held",
    async (phase) =>
      fixture(async (f) => {
        if (phase === "held") f.updater.setHeldJobs(["unreleased-job"]);
        else {
          const snapshot = f.updater.snapshot.bind(f.updater);
          vi.spyOn(f.updater, "snapshot").mockImplementation(() => ({ ...snapshot(), phase }));
        }
        expect(await f.operations.start(input)).toMatchObject({ phase: "refused", code: "busy" });
        expect(f.updater.reserveCheckAdmission).not.toHaveBeenCalled();
        expect(f.check).not.toHaveBeenCalled();
        expect(f.prepareCalls()).toBe(0);
      }),
  );

  it("returns durable acceptance and replays the same ID without another preparation", async () =>
    fixture(async (f) => {
      expect((await f.operations.start(input)).phase).toBe("accepted");
      await vi.waitFor(() => expect(f.prepareCalls()).toBe(1));
      expect((await f.operations.start(input)).jobId).toBe(f.jobId());
      f.finish("completed");
      await vi.waitFor(async () => expect((await f.receipt())?.phase).toBe("completed"));
      expect((await f.receipt())?.staged?.sourceSha).toBe(candidate.source);
      expect(f.install).not.toHaveBeenCalled();
    }));
  it("retains the exact downgrade reason without starting a preparation worker", async () =>
    fixture(async (f) => {
      f.check.mockResolvedValueOnce({
        state: "blocked",
        reason: "target-older-than-installed",
        retryAfterMs: 0,
      });
      expect((await f.operations.start(input)).phase).toBe("accepted");
      expect(await f.receiptAt("failed")).toMatchObject({
        code: "target-older-than-installed",
        message: expect.stringContaining("older than the build running on this host"),
      });
      expect(await f.receipt()).not.toHaveProperty("jobId");
      expect(f.prepareCalls()).toBe(0);
      expect(f.install).not.toHaveBeenCalled();
    }));
  it("Stop does not settle or release admission when only the caller was aborted", async () =>
    fixture(async (f) => {
      await f.operations.start(input);
      await vi.waitFor(() => expect(f.prepareCalls()).toBe(1));
      const stop = {
        operationId: input.operationId,
        environmentId: input.environmentId,
        stopId: NodeCrypto.randomUUID(),
      };
      expect((await f.operations.stop(stop)).phase).toBe("stop-requested");
      expect(
        (await f.operations.stop({ ...stop, stopId: NodeCrypto.randomUUID() })).stop?.stopId,
      ).toBe(stop.stopId);
      expect(f.updater.admissionHeld()).toBe(true);
      expect(
        (await f.operations.start({ ...input, operationId: NodeCrypto.randomUUID() })).code,
      ).toBe("busy");
      f.finish("stopped");
      await vi.waitFor(async () => expect((await f.receipt())?.phase).toBe("stopped"));
      expect(f.updater.snapshot().phase).toBe("available");
      expect(f.prepareCalls()).toBe(1);
    }));
  it("keeps scratch deletion separate from positive worker release", async () =>
    fixture(async (f) => {
      await f.operations.start(input);
      await vi.waitFor(() => expect(f.prepareCalls()).toBe(1));
      await f.operations.stop({ ...input, stopId: NodeCrypto.randomUUID() });
      f.finish("unreaped");
      await vi.waitFor(async () => expect((await f.receipt())?.phase).toBe("held"));
      await f.operations.reconcile();
      expect(f.updater.admissionHeld()).toBe(true);
      f.scratchDeleted();
      await f.operations.sweep();
      expect((await f.receipt())?.phase).toBe("held");
      expect(f.updater.admissionHeld()).toBe(true);
      f.release();
      await f.operations.sweep();
      expect((await f.receipt())?.phase).toBe("stopped");
      expect(f.updater.admissionHeld()).toBe(false);
    }));
  it("stops before spawn while exact discovery is in flight", async () =>
    fixture(async (f) => {
      const check = deferred<void>();
      f.blockCheck(check.promise);
      await f.operations.start(input);
      await f.checkEntered;
      await f.operations.stop({ ...input, stopId: NodeCrypto.randomUUID() });
      check.resolve();
      expect(await f.receiptAt("stopped")).not.toHaveProperty("jobId");
      expect(f.prepareCalls()).toBe(0);
    }));
  it("interrupts a restart during exact catalog discovery without holding or replaying work", async () =>
    fixture(async (f) => {
      // Leave the crashed controller parked at this read-only boundary for the whole test.
      f.blockCheck(deferred<void>().promise);
      expect((await f.operations.start(input)).phase).toBe("accepted");
      await f.checkEntered;
      const restarted = await make(f.base, false);
      await restarted.operations.reconcile();
      expect(await restarted.receipt()).toMatchObject({ phase: "interrupted" });
      expect(await restarted.receipt()).not.toHaveProperty("jobId");
      expect(restarted.updater.admissionHeld()).toBe(false);
      expect((await restarted.operations.start(input)).phase).toBe("interrupted");
      expect(restarted.check).not.toHaveBeenCalled();
      expect(restarted.prepareCalls()).toBe(0);
    }));
  it("honors the recorded Stop before the reservation callback can attach a job", async () =>
    fixture(async (f) => {
      const release = deferred<void>();
      f.blockReservation(release.promise);
      await f.operations.start(input);
      await f.reservationEntered;
      try {
        const stop = await f.operations.stop({ ...input, stopId: NodeCrypto.randomUUID() });
        expect(stop.phase).toBe("stop-requested");
        expect(stop.jobId).toBeUndefined();
      } finally {
        release.resolve();
        await f.receiptAt("stopped");
      }
      expect((await f.receipt())?.jobId).toBeUndefined();
      expect(f.prepareCalls()).toBe(0);
    }));
  it.each(["before", "after"] as const)(
    "recovers a durable reservation interrupted %s receipt attachment without starting a worker",
    async (side) =>
      fixture(async (f) => {
        const observed = vi.fn();
        const cacheRoot = NodePath.join(f.base, "cache");
        const supervisor = () =>
          makePreparationSupervisor({
            baseDir: f.base,
            cacheRoot,
            command: { file: "/nonexistent-jones-test-worker", args: [] },
            observe: observed,
          });
        await f.ledger.accept(input);
        const jobId = NodeCrypto.randomUUID();
        const lifecycle = NodePath.join(
          f.base,
          "runtime",
          "jones-preparation",
          "lifecycle",
          `${jobId}.json`,
        );
        const entered = deferred<void>();
        const allowBinding = deferred<void>();
        const bound = deferred<void>();
        const returnBinding = deferred<void>();
        const controller = new AbortController();
        const original = supervisor();
        const job = original.start(
          {
            kind: "stage",
            candidate,
            cacheRoot,
            dbPath: NodePath.join(f.base, "db"),
            activeVersion: "0.0.0-preview.20261002.1",
            host: { platform: "linux", architecture: "x64" },
          },
          {
            jobId,
            signal: controller.signal,
            onReserved: async (reservedId) => {
              entered.resolve();
              await allowBinding.promise;
              controller.signal.throwIfAborted();
              await f.ledger.transition(input.operationId, (r) => ({
                ...r,
                phase: "running",
                jobId: reservedId,
              }));
              bound.resolve();
              await returnBinding.promise;
            },
          },
        );
        const outcome = job.outcome.catch(() => undefined);
        try {
          await entered.promise;
          expect(await f.receipt()).toMatchObject({ phase: "accepted" });
          expect(await f.receipt()).not.toHaveProperty("jobId");
          expect(JSON.parse(await NodeFSP.readFile(lifecycle, "utf8"))).toMatchObject({
            jobId,
            state: "not-started",
          });
          // A competing controller cannot treat the live reservation as released, even if it knows the ID.
          expect(await supervisor().reconcile!(jobId)).toBe("unknown");
          if (side === "after") {
            allowBinding.resolve();
            await bound.promise;
            expect(await f.receipt()).toMatchObject({ phase: "running", jobId });
            expect(JSON.parse(await NodeFSP.readFile(lifecycle, "utf8"))).toMatchObject({
              jobId,
              state: "not-started",
            });
            await expect(
              NodeFSP.stat(NodePath.join(preparationJobsDirectory(f.base), jobId)),
            ).rejects.toMatchObject({ code: "ENOENT" });
          }
        } finally {
          controller.abort();
          allowBinding.resolve();
          returnBinding.resolve();
          await job.settled;
          await outcome;
          await original.sweepIfNeeded?.().catch(() => undefined);
        }
        expect(await job.settled).toEqual({ jobId, kind: "not-started" });
        expect(observed.mock.calls.every(([exit]) => exit.kind !== "stage")).toBe(true);
        const restartedPreparation = supervisor();
        expect(await restartedPreparation.reconcile!(jobId)).toEqual({
          jobId,
          kind: "not-started",
        });
        const restarted = await make(f.base, false, undefined, restartedPreparation);
        await restarted.operations.reconcile();
        expect(await restarted.receipt()).toMatchObject({
          phase: "interrupted",
          ...(side === "after" ? { jobId } : {}),
        });
        if (side === "before") expect(await restarted.receipt()).not.toHaveProperty("jobId");
        expect(restarted.updater.admissionHeld()).toBe(false);
        expect((await restarted.operations.start(input)).phase).toBe("interrupted");
        expect(restarted.prepareCalls()).toBe(0);
        if (side === "before") {
          // A crash can leave scratch with a reservation and no logical owner. Cleanup still
          // requires the exact job lock and positive release evidence.
          const scratch = NodePath.join(preparationJobsDirectory(f.base), jobId);
          await NodeFSP.mkdir(scratch, { recursive: true });
          const swept = await sweepPreparationLeftovers({
            baseDir: f.base,
            cacheRoot,
            keepJob: NodeCrypto.randomUUID(),
          });
          expect(swept.removedJobIds).toEqual([jobId]);
          await expect(NodeFSP.stat(scratch)).rejects.toMatchObject({ code: "ENOENT" });
        }
      }),
  );
  it("records a late Stop without undoing a published stage", async () =>
    fixture(async (f) => {
      await f.operations.start(input);
      await vi.waitFor(() => expect(f.prepareCalls()).toBe(1));
      f.finish("completed");
      await vi.waitFor(async () => expect((await f.receipt())?.phase).toBe("completed"));
      const stopped = await f.operations.stop({ ...input, stopId: NodeCrypto.randomUUID() });
      expect(stopped.phase).toBe("completed");
      expect(stopped.stop?.result).toBe("too-late");
      expect(stopped.staged).toBeDefined();
    }));
  it("fences a lost send and never calls the worker for a late replay", async () =>
    fixture(async (f) => {
      const fenced = await f.operations.fence(input);
      expect(fenced).toMatchObject({ ...input, phase: "fenced" });
      expect(fenced.acceptedAt).toBeUndefined();
      expect((await f.operations.start(input)).phase).toBe("fenced");
      expect(f.prepareCalls()).toBe(0);
    }));
  it("keeps an unrecognized Stop unresolved without inventing an action receipt", async () =>
    fixture(async (f) => {
      await expect(
        f.operations.stop({
          operationId: input.operationId,
          environmentId: input.environmentId,
          stopId: NodeCrypto.randomUUID(),
        }),
      ).rejects.toThrow("no acceptance receipt");
      expect(await f.receipt()).toBeNull();
      expect(f.prepareCalls()).toBe(0);
    }));
  it("never settles restarted dispatch from native absence, and follows only exact launcher evidence", async () =>
    fixture(async (f) => {
      const installInput: JonesOperationStartInput = {
        ...input,
        action: {
          kind: "install",
          stagedHandle: "stage",
          currentVersion: "old",
          targetSource: candidate.source,
        },
      };
      await f.ledger.accept(installInput);
      await f.ledger.transition(input.operationId, (r) => ({ ...r, phase: "running" }));
      await f.operations.reconcile();
      expect(await f.receipt()).toMatchObject({ phase: "running", code: "launcher-unconfirmed" });
      expect(f.install).not.toHaveBeenCalled();
      const binding = {
        environmentId: input.environmentId,
        expectedInstalledSource: input.expectedInstalledSource,
        stagedHandle: "stage",
        currentVersion: "old",
        targetSource: candidate.source,
      };
      f.setNative({ state: "pending", operationId: input.operationId, binding });
      await f.operations.reconcile();
      expect((await f.receipt())?.phase).toBe("handed-off");
      f.setNative({
        state: "committed",
        operationId: input.operationId,
        binding,
        updateId: input.operationId,
      });
      await f.operations.reconcile();
      expect((await f.receipt())?.phase).toBe("completed");
    }));
  it.each(["handed-off", "completed"] as const)(
    "refreshes only the exact manually requested terminal launcher outcome from %s",
    async (phase) =>
      fixture(async (f) => {
        const action = {
          kind: "install",
          stagedHandle: "stage",
          currentVersion: "old",
          targetSource: candidate.source,
        } as const;
        const binding = {
          environmentId: input.environmentId,
          expectedInstalledSource: input.expectedInstalledSource,
          stagedHandle: action.stagedHandle,
          currentVersion: action.currentVersion,
          targetSource: action.targetSource,
        };
        const id = input.operationId;
        const other = NodeCrypto.randomUUID();
        await f.ledger.accept({ ...input, action });
        await f.ledger.accept({ ...input, operationId: other, action });
        await f.ledger.transition(id, (r) => ({ ...r, phase: "running" }));
        const held = await f.ledger.transition(id, (r) => ({
          ...r,
          phase,
          updateId: id,
          ...(phase === "handed-off" ? { code: "launcher-unconfirmed" } : {}),
        }));
        const unrelated = await f.ledger.read(other);
        const read = { environmentId: input.environmentId, operationIds: [id] };
        const refresh = { ...read, refreshInstallOutcome: id };
        expect((await f.operations.read(read)).receipts[0]?.receipt).toEqual(held);
        await f.operations.read({ ...refresh, operationIds: [id, other] });
        expect(f.reconcileNative).not.toHaveBeenCalled();

        f.setNative({ state: "pending", operationId: id, binding, updateId: id });
        expect((await f.operations.read(refresh)).receipts[0]?.receipt).toEqual(held);
        const outcome = {
          status: "committed",
          fromVersion: "old",
          targetVersion: candidate.version,
        } as const;
        const committed = {
          state: "committed",
          operationId: id,
          binding,
          updateId: id,
          outcome,
        } as const;
        f.setNative({ ...committed, binding: { ...binding, stagedHandle: "other-stage" } });
        expect((await f.operations.read(refresh)).receipts[0]?.receipt).toEqual(held);

        f.setNative(committed);
        const result = (await f.operations.read(refresh)).receipts[0]?.receipt;
        expect(result).toMatchObject({
          operationId: id,
          phase: "completed",
          updateId: id,
          outcome,
        });
        expect(result?.code).toBeUndefined();
        expect(await f.receipt()).toEqual(result);
        expect(await f.ledger.read(other)).toEqual(unrelated);
        expect(f.reconcileNative.mock.calls).toEqual([[id], [id], [id]]);
        expect(f.install).not.toHaveBeenCalled();
      }),
  );
  it("only releases an install with no reservation after ordered launcher rejection", async () =>
    fixture(async (f) => {
      const operationId = input.operationId;
      const action = {
        kind: "install",
        stagedHandle: "stage",
        currentVersion: "old",
        targetSource: candidate.source,
      } as const;
      await f.ledger.accept({ ...input, action });
      await f.ledger.transition(operationId, (receipt) => ({ ...receipt, phase: "running" }));
      await f.operations.reconcile();
      expect(f.updater.admissionHeld()).toBe(true);
      const request = {
        environmentId: input.environmentId,
        operationIds: [operationId],
        refreshInstallOutcome: operationId,
      };
      expect((await f.operations.read(request)).receipts[0]?.receipt).toMatchObject({
        phase: "running",
        code: "launcher-unconfirmed",
      });
      expect(f.updater.admissionHeld()).toBe(true);
      f.resolveNativeHandoff.mockResolvedValue("rejected");
      expect((await f.operations.read(request)).receipts[0]?.receipt).toMatchObject({
        phase: "refused",
        code: "launcher-rejected",
      });
      expect((await f.receipt())?.phase).toBe("refused");
      expect(f.updater.admissionHeld()).toBe(false);
      expect(f.install).not.toHaveBeenCalled();
    }));

  it("retires only a stage proved to belong to this plan and recognizes a retry", async () =>
    fixture(async (f) => {
      const planId = NodeCrypto.randomUUID();
      const operationId = input.operationId;
      const stagedHandle = "owned-stage";
      const correlation = { planId, deviceKey: input.environmentId };
      const snapshot = f.updater.snapshot();
      const retire = vi
        .spyOn(f.updater, "retireStagedOperation")
        .mockResolvedValue({ retired: true });
      vi.spyOn(f.updater, "snapshot").mockReturnValue({
        ...snapshot,
        capability: { ...snapshot.capability, abandonStage: 1 },
      });
      await f.ledger.accept({ ...input, correlation });
      await f.ledger.transition(operationId, (receipt) => ({ ...receipt, phase: "running" }));
      await f.ledger.transition(operationId, (receipt) => ({ ...receipt, phase: "adopting" }));
      await f.ledger.transition(operationId, (receipt) => ({
        ...receipt,
        phase: "completed",
        staged: {
          stagedHandle,
          artifactId: 4,
          sourceSha: candidate.source,
          sourceTree: candidate.tree,
          createdByOperation: true,
        },
      }));
      const abandon = {
        planId,
        operationId,
        environmentId: input.environmentId,
        stagedHandle,
        confirmed: true,
      } as const;
      expect(
        (await f.operations.abandonStage({ ...abandon, planId: NodeCrypto.randomUUID() })).receipt,
      ).toBeNull();
      expect(
        (await f.operations.abandonStage({ ...abandon, stagedHandle: "other" })).receipt,
      ).toBeNull();
      expect(retire).not.toHaveBeenCalled();
      const settled = await f.operations.abandonStage(abandon);
      expect(settled.receipt?.stageRetiredAt).toEqual(expect.any(String));
      expect(await f.operations.abandonStage(abandon)).toEqual(settled);
      expect(retire).toHaveBeenCalledTimes(1);
    }));

  it("preserves a recorded launcher acceptance when its native files are missing", async () =>
    fixture(async (f) => {
      const operationId = input.operationId;
      await f.ledger.accept({
        ...input,
        action: {
          kind: "install",
          stagedHandle: "stage",
          currentVersion: "old",
          targetSource: candidate.source,
        },
      });
      await f.ledger.transition(operationId, (receipt) => ({ ...receipt, phase: "running" }));
      await f.ledger.transition(operationId, (receipt) => ({
        ...receipt,
        phase: "handed-off",
        updateId: operationId,
      }));
      await f.operations.reconcile();
      f.resolveNativeHandoff.mockResolvedValue("rejected");
      const result = await f.operations.read({
        environmentId: input.environmentId,
        operationIds: [operationId],
        refreshInstallOutcome: operationId,
      });
      expect(result.receipts[0]?.receipt).toMatchObject({
        phase: "handed-off",
        updateId: operationId,
      });
      expect(f.updater.admissionHeld()).toBe(true);
      expect(f.resolveNativeHandoff).not.toHaveBeenCalled();
    }));

  it.each(["legacy", "spawning", "unreadable"] as const)(
    "holds a restarted download with %s worker evidence",
    async (evidence) =>
      fixture(async (f) => {
        const jobId = NodeCrypto.randomUUID();
        await f.ledger.accept(input);
        await f.ledger.transition(input.operationId, (r) => ({
          ...r,
          phase: "running",
          jobId,
        }));
        const preparation = makePreparationSupervisor({
          baseDir: f.base,
          cacheRoot: NodePath.join(f.base, "cache"),
          command: { file: "unused", args: [] },
        });
        if (evidence !== "legacy") {
          const launch = await preparationLifecycle(f.base, jobId).reserve("stage");
          if (evidence === "spawning") await launch.spawning();
          else
            await NodeFSP.chmod(
              NodePath.join(f.base, "runtime", "jones-preparation", "lifecycle", `${jobId}.json`),
              0o622,
            );
        }
        const restarted = await make(f.base, false, undefined, preparation);
        await restarted.operations.reconcile();
        expect(await restarted.receipt()).toMatchObject({ phase: "held", code: "unreaped", jobId });
        expect(restarted.updater.admissionHeld()).toBe(true);
        expect(
          (
            await restarted.operations.read({
              environmentId: input.environmentId,
              operationIds: [NodeCrypto.randomUUID()],
            })
          ).receipts[0]?.receipt,
        ).toBeNull();
      }),
  );
});

it("persists precondition refusals and prevents stale IDs from becoming a later dispatch", async () =>
  fixture(async (f) => {
    const wrong = { ...input, expectedInstalledSource: "f".repeat(40) };
    expect((await f.operations.start(wrong)).code).toBe("stale-binding");
    expect((await f.operations.start(input)).code).toBe("operation-conflict");
    expect((await f.ledger.read(input.operationId))?.expectedInstalledSource).toBe(
      wrong.expectedInstalledSource,
    );
    expect(f.prepareCalls()).toBe(0);
    await expect(
      f.operations.fence({
        ...input,
        operationId: NodeCrypto.randomUUID(),
        environmentId: EnvironmentId.make("another-host"),
      }),
    ).rejects.toThrow("environment");
  }));

it("does not turn a mismatched native blocked response into terminal failure", async () =>
  fixture(async (f) => {
    const action = {
      kind: "install" as const,
      stagedHandle: "stage",
      currentVersion: "old",
      targetSource: candidate.source,
    };
    await f.ledger.accept({ ...input, action });
    await f.ledger.transition(input.operationId, (r) => ({ ...r, phase: "running" }));
    f.setNative({
      operationId: input.operationId,
      state: "blocked",
      updateId: NodeCrypto.randomUUID(),
    });
    await f.operations.reconcile();
    expect(await f.receipt()).toMatchObject({ phase: "running", code: "launcher-unreadable" });
    expect(f.updater.admissionHeld()).toBe(true);
    expect(f.install).not.toHaveBeenCalled();
  }));

it("dispatches a reviewed install once with manual origin and retains uncertainty on a lost native reply", async () =>
  fixture(async (f) => {
    await f.operations.start(input);
    await vi.waitFor(() => expect(f.prepareCalls()).toBe(1));
    f.finish("completed");
    await vi.waitFor(async () => expect((await f.receipt())?.phase).toBe("completed"));
    await f.operations.read({
      environmentId: input.environmentId,
      operationIds: [input.operationId],
    });
    const operationId = NodeCrypto.randomUUID();
    const request: JonesOperationStartInput = {
      ...input,
      operationId,
      action: {
        kind: "install",
        stagedHandle: "stage",
        currentVersion: f.updater.snapshot().currentVersion!,
        targetSource: candidate.source,
      },
    };
    expect((await f.operations.start(request)).phase).toBe("accepted");
    await vi.waitFor(async () =>
      expect((await f.ledger.read(operationId))?.code).toBe("launcher-unconfirmed"),
    );
    expect(f.install).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({ operationId, origin: "manual", stagedHandle: "stage" }),
    );
    expect((await f.operations.start(request)).phase).toBe("running");
    expect(f.install).toHaveBeenCalledTimes(1);
    expect(f.updater.admissionHeld()).toBe(true);
  }));

it("settles a restart before dispatch without replaying its effect", async () =>
  fixture(async (f) => {
    await f.ledger.accept(input);
    await f.operations.reconcile();
    expect((await f.receipt())?.phase).toBe("interrupted");
    expect(f.prepareCalls()).toBe(0);
    expect(f.install).not.toHaveBeenCalled();
  }));

it("preserves a refused Stop when restarting before native install dispatch", async () =>
  fixture(async (f) => {
    const installRequest: JonesOperationStartInput = {
      ...input,
      operationId: NodeCrypto.randomUUID(),
      action: {
        kind: "install",
        stagedHandle: "stage",
        currentVersion: f.updater.snapshot().currentVersion!,
        targetSource: candidate.source,
      },
    };
    await f.ledger.accept(installRequest);
    const stopped = await f.operations.stop({
      operationId: installRequest.operationId,
      environmentId: installRequest.environmentId,
      stopId: NodeCrypto.randomUUID(),
    });
    expect(stopped.phase).toBe("accepted");
    expect(stopped.stop?.result).toBe("not-stoppable-phase");

    const restarted = await make(f.base, false);
    await restarted.operations.reconcile();
    expect(await restarted.ledger.read(installRequest.operationId)).toMatchObject({
      phase: "interrupted",
      stop: stopped.stop,
    });
    expect(restarted.install).not.toHaveBeenCalled();
    expect(restarted.reconcileNative).not.toHaveBeenCalled();
    expect(restarted.updater.admissionHeld()).toBe(false);
    expect((await restarted.operations.start(input)).phase).toBe("accepted");
    await restarted.prepareStarted;
    restarted.finish("completed");
    await restarted.receiptAt("completed");
  }));

it("keeps a generic blocked lost reply held through absent, restart, pending and committed without redispatch", async () =>
  fixture(async (f) => {
    await f.operations.start(input);
    await vi.waitFor(() => expect(f.prepareCalls()).toBe(1));
    f.finish("completed");
    await vi.waitFor(async () => expect((await f.receipt())?.phase).toBe("completed"));
    f.install.mockImplementation(async () => ({ ...f.updater.snapshot(), phase: "blocked" }));
    const request: JonesOperationStartInput = {
      ...input,
      operationId: NodeCrypto.randomUUID(),
      action: {
        kind: "install",
        stagedHandle: "stage",
        currentVersion: f.updater.snapshot().currentVersion!,
        targetSource: candidate.source,
      },
    };
    await f.operations.start(request);
    await vi.waitFor(async () =>
      expect(await f.ledger.read(request.operationId)).toMatchObject({
        phase: "running",
        code: "launcher-unconfirmed",
      }),
    );
    expect(f.updater.admissionHeld()).toBe(true);
    await f.operations.read({
      environmentId: input.environmentId,
      operationIds: [request.operationId],
    });
    await f.operations.start(request);
    expect(f.install).toHaveBeenCalledOnce();
    const restarted = await make(f.base, false);
    await restarted.operations.reconcile();
    expect(await restarted.ledger.read(request.operationId)).toMatchObject({
      phase: "running",
      code: "launcher-unconfirmed",
    });
    expect(restarted.updater.admissionHeld()).toBe(true);
    if (request.action.kind !== "install") throw new Error("Expected install fixture.");
    const binding = {
      environmentId: request.environmentId,
      expectedInstalledSource: request.expectedInstalledSource,
      stagedHandle: request.action.stagedHandle,
      currentVersion: request.action.currentVersion,
      targetSource: request.action.targetSource,
    };
    restarted.setNative({ state: "pending", operationId: request.operationId, binding });
    await restarted.operations.reconcile();
    expect(await restarted.ledger.read(request.operationId)).toMatchObject({
      phase: "handed-off",
      updateId: request.operationId,
    });
    restarted.setNative({
      state: "committed",
      operationId: request.operationId,
      binding,
      updateId: request.operationId,
    });
    await restarted.operations.reconcile();
    expect(await restarted.ledger.read(request.operationId)).toMatchObject({
      phase: "completed",
      updateId: request.operationId,
    });
    expect((await restarted.ledger.read(request.operationId))?.code).toBeUndefined();
    expect(restarted.updater.admissionHeld()).toBe(false);
    expect(f.install).toHaveBeenCalledOnce();
    expect(restarted.install).not.toHaveBeenCalled();
  }));

it("settles an exactly bound typed refusal proved before dispatch", async () =>
  fixture(async (f) => {
    await f.operations.start(input);
    await vi.waitFor(() => expect(f.prepareCalls()).toBe(1));
    f.finish("completed");
    await vi.waitFor(async () => expect((await f.receipt())?.phase).toBe("completed"));
    f.install.mockImplementation(async (request) => {
      request.onBeforeDispatchRefusal?.({ ...request, kind: "before-dispatch" });
      return { ...f.updater.snapshot(), phase: "blocked" };
    });
    const request: JonesOperationStartInput = {
      ...input,
      operationId: NodeCrypto.randomUUID(),
      action: {
        kind: "install",
        stagedHandle: "stage",
        currentVersion: f.updater.snapshot().currentVersion!,
        targetSource: candidate.source,
      },
    };
    await f.operations.start(request);
    await vi.waitFor(async () =>
      expect(await f.ledger.read(request.operationId)).toMatchObject({
        phase: "refused",
        code: "install-refused",
      }),
    );
    expect(f.updater.admissionHeld()).toBe(false);
    await f.operations.start(request);
    expect(f.install).toHaveBeenCalledOnce();
  }));

it("holds a reaped preparation whose native cleanup is unconfirmed", async () =>
  fixture(async (f) => {
    await f.operations.start(input);
    await vi.waitFor(() => expect(f.prepareCalls()).toBe(1));
    await f.operations.stop({ ...input, stopId: NodeCrypto.randomUUID() });
    f.finish("cleanup-unconfirmed");
    await vi.waitFor(async () =>
      expect(await f.receipt()).toMatchObject({
        phase: "held",
        code: "cleanup-unconfirmed",
        stop: { result: "held" },
      }),
    );
    expect(f.updater.snapshot().phase).toBe("blocked");
    const held = (await f.receipt())!;
    vi.setSystemTime(Date.parse(held.updatedAt) + 60_000);
    try {
      await f.operations.reconcile();
      expect(await f.receipt()).toEqual(held);
    } finally {
      vi.useRealTimers();
    }
    f.scratchDeleted();
    await f.operations.sweep();
    expect(f.updater.admissionHeld()).toBe(true);
    expect(
      (await f.operations.start({ ...input, operationId: NodeCrypto.randomUUID() })).code,
    ).toBe("busy");
    expect(f.prepareCalls()).toBe(1);
    expect(f.install).not.toHaveBeenCalled();
    f.release();
    await f.operations.reconcile();
    expect(await f.receipt()).toMatchObject({ phase: "stopped", stop: { result: "stopped" } });
    expect((await f.receipt())?.code).toBeUndefined();
    expect(f.updater.admissionHeld()).toBe(false);
  }));

it.each(["transfer", "native-effects"] as const)(
  "reconciles durable %s evidence through supervisor, free-lock sweep, scratch loss and restart",
  async (phase) =>
    fixture(async (f) => {
      const jobId = NodeCrypto.randomUUID();
      const pid = 54321;
      let descendantAlive = phase === "transfer";
      const kill = vi.spyOn(process, "kill").mockImplementation((requested, signal) => {
        if (signal !== 0 || (requested !== pid && requested !== -pid))
          throw new Error("Unexpected synthetic process probe.");
        if (requested === -pid && descendantAlive) return true;
        throw Object.assign(new Error("Synthetic process absence."), { code: "ESRCH" });
      });
      try {
        const lifecycle = preparationLifecycle(f.base, jobId);
        const launch = await lifecycle.reserve("stage");
        await launch.started({ pid, processGroup: pid });
        await lifecycle.effects(pid, phase);
        const scratch = NodePath.join(preparationJobsDirectory(f.base), jobId);
        await NodeFSP.mkdir(scratch, { recursive: true });
        await f.ledger.accept(input);
        await f.ledger.transition(input.operationId, (r) => ({ ...r, phase: "running", jobId }));
        const supervisor = () =>
          makePreparationSupervisor({
            baseDir: f.base,
            cacheRoot: NodePath.join(f.base, "cache"),
            command: { file: "unused-synthetic-worker", args: [] },
          });
        const bind = (owner: typeof f, preparation: PreparationSupervisor) =>
          createTargetOperations({
            key: f.base,
            ledger: owner.ledger,
            fleetStore: owner.fleetStore,
            updater: owner.updater,
            preparation,
            install: owner.install,
            refreshPolicy: async () => {},
            reconcileNative: async () => ({ state: "absent", operationId: input.operationId }),
            onFailure: () => {},
          });
        const preparation = supervisor();
        const operations = bind(f, preparation);
        await operations.stop({ ...input, stopId: NodeCrypto.randomUUID() });
        await operations.reconcile();
        expect(await f.receipt()).toMatchObject({ phase: "held", stop: { result: "held" } });
        expect(await preparation.reconcile?.(jobId)).toMatchObject({
          jobId,
          kind: descendantAlive ? "unreaped" : "cleanup-unconfirmed",
        });
        // Both old owner locks are free. The real sweep must retain unresolved scratch.
        expect(
          await sweepPreparationLeftovers({
            baseDir: f.base,
            cacheRoot: NodePath.join(f.base, "cache"),
            keepJob: NodeCrypto.randomUUID(),
          }),
        ).toMatchObject({ removedJobIds: [], retainedJobIds: [jobId] });
        await NodeFSP.rm(scratch, { recursive: true });
        const restarted = await make(f.base, false);
        const nextPreparation = supervisor();
        const next = bind(restarted, nextPreparation);
        await next.reconcile();
        expect(await restarted.receipt()).toMatchObject({
          phase: "held",
          stop: { result: "held" },
        });
        expect(restarted.updater.admissionHeld()).toBe(true);
        expect((await next.start({ ...input, operationId: NodeCrypto.randomUUID() })).code).toBe(
          "busy",
        );
        await restarted.updater.install({
          stagedHandle: "not-admitted",
          environmentId: input.environmentId,
          currentVersion: "old",
        });
        expect(restarted.prepareCalls()).toBe(0);
        expect(restarted.install).not.toHaveBeenCalled();
        descendantAlive = false;
        if (phase === "native-effects") await lifecycle.effects(pid, "clean");
        await next.reconcile();
        expect(await restarted.receipt()).toMatchObject({
          phase: "stopped",
          stop: { result: "stopped" },
        });
        expect(restarted.updater.admissionHeld()).toBe(false);
        expect(await nextPreparation.reconcile?.(jobId)).toMatchObject({ jobId, kind: "stopped" });
      } finally {
        kill.mockRestore();
      }
    }),
);

it("holds interrupted stage publication even after the worker is gone", async () =>
  fixture(async (f) => {
    await f.ledger.accept(input);
    await f.ledger.transition(input.operationId, (r) => ({
      ...r,
      phase: "running",
      jobId: NodeCrypto.randomUUID(),
    }));
    await f.ledger.transition(input.operationId, (r) => ({ ...r, phase: "adopting" }));
    await f.operations.reconcile();
    expect(await f.receipt()).toMatchObject({ phase: "held", code: "adoption-unconfirmed" });
    expect(f.updater.admissionHeld()).toBe(true);
  }));

it("keeps adoption admission held when a later scan proves worker release", async () =>
  fixture(async (f) => {
    const jobId = NodeCrypto.randomUUID();
    let unresolved = [jobId];
    const preparation: PreparationSupervisor = {
      ...f.preparation,
      unreleasedJobs: async () => unresolved,
      requestSweep: vi.fn(),
    };
    const operations = createTargetOperations({
      key: f.base,
      ledger: f.ledger,
      fleetStore: f.fleetStore,
      updater: f.updater,
      preparation,
      refreshPolicy: f.refreshPolicy,
      install: f.install,
      reconcileNative: f.reconcileNative,
      onFailure: () => {},
    });
    await f.ledger.accept(input);
    await f.ledger.transition(input.operationId, (r) => ({ ...r, phase: "running", jobId }));
    await f.ledger.transition(input.operationId, (r) => ({ ...r, phase: "adopting" }));
    await operations.reconcile();
    unresolved = [];
    await operations.reconcile();
    expect(await f.receipt()).toMatchObject({ phase: "held", code: "adoption-unconfirmed" });
    expect(f.updater.admissionHeld()).toBe(true);
    expect((await operations.start({ ...input, operationId: NodeCrypto.randomUUID() })).code).toBe(
      "busy",
    );
    expect(f.prepareCalls()).toBe(0);
    expect(f.install).not.toHaveBeenCalled();
  }));

it("lets adoption win a Stop race only after its durable marker and finishes with too-late", async () =>
  fixture(async (f) => {
    const publication = deferred<void>();
    f.blockPublication(publication.promise);
    await f.operations.start(input);
    await vi.waitFor(() => expect(f.prepareCalls()).toBe(1));
    try {
      f.finish("completed");
      await vi.waitFor(async () => expect((await f.receipt())?.phase).toBe("adopting"));
      const receipt = await f.operations.stop({ ...input, stopId: NodeCrypto.randomUUID() });
      expect(receipt).toMatchObject({ phase: "adopting", stop: { result: "not-stoppable-phase" } });
      publication.resolve();
      await vi.waitFor(async () =>
        expect(await f.receipt()).toMatchObject({
          phase: "completed",
          stop: { result: "too-late" },
        }),
      );
      expect(f.updater.snapshot().phase).toBe("staged");
      expect(f.install).not.toHaveBeenCalled();
    } finally {
      publication.resolve();
    }
  }));

it("refuses an expired reviewed recovery binding before native dispatch and records requested versus observed method", async () =>
  fixture(async (f) => {
    await f.operations.start(input);
    await vi.waitFor(() => expect(f.prepareCalls()).toBe(1));
    f.finish("completed");
    await vi.waitFor(async () => expect((await f.receipt())?.phase).toBe("completed"));
    const binding = {
      environmentId: input.environmentId,
      expectedInstalledSource: input.expectedInstalledSource,
      stagedHandle: "stage",
      currentVersion: f.updater.snapshot().currentVersion!,
    };
    const measured = await f.recovery.preflight(binding);
    const install = (preflightId: string): JonesOperationStartInput => ({
      ...input,
      operationId: NodeCrypto.randomUUID(),
      action: {
        kind: "install",
        targetSource: candidate.source,
        stagedHandle: binding.stagedHandle,
        currentVersion: binding.currentVersion,
        recovery: { policy: "clone-required", preflightId },
      },
    });
    f.expireRecovery();
    const stale = await f.operations.start(install(measured.preflightId));
    expect(stale).toMatchObject({ phase: "refused", code: "recovery-preflight-expired" });
    expect(f.install).not.toHaveBeenCalled();
    const fresh = await f.recovery.preflight(binding);
    const normal = install(fresh.preflightId);
    if (normal.action.kind !== "install") throw new Error("Expected install fixture.");
    expect(
      await f.operations.start({
        ...normal,
        operationId: NodeCrypto.randomUUID(),
        action: {
          ...normal.action,
          recovery: { policy: "clone-or-copy", preflightId: fresh.preflightId },
        },
      }),
    ).toMatchObject({ phase: "refused", code: "recovery-policy-refused" });
    expect(f.install).not.toHaveBeenCalled();
    f.setNative({
      state: "committed",
      operationId: normal.operationId,
      binding: { ...binding, targetSource: candidate.source },
    });
    expect((await f.operations.start(normal)).phase).toBe("accepted");
    await vi.waitFor(async () =>
      expect((await f.ledger.read(normal.operationId))?.phase).toBe("completed"),
    );
    expect(f.install).toHaveBeenCalledOnce();
    expect(f.install.mock.calls[0]?.[0].recovery).toEqual({
      policy: "clone-required",
      preflightId: fresh.preflightId,
    });
    expect(await f.ledger.read(normal.operationId)).toMatchObject({
      recovery: { requested: "clone-required", observed: "clone" },
    });
    await f.operations.start(normal);
    expect(f.install).toHaveBeenCalledOnce();
  }, true));

it.each(["prepare", "handoff"] as const)(
  "keeps a host %s failure terminal only with positive before-dispatch proof",
  async (phase) =>
    fixture(
      async (f) => {
        await f.operations.start(input);
        await vi.waitFor(() => expect(f.prepareCalls()).toBe(1));
        f.finish("completed");
        await vi.waitFor(async () => expect((await f.receipt())?.phase).toBe("completed"));
        f.install.mockImplementation((request) => f.updater.install(request));
        const request: JonesOperationStartInput = {
          ...input,
          operationId: NodeCrypto.randomUUID(),
          action: {
            kind: "install",
            stagedHandle: "stage",
            currentVersion: f.updater.snapshot().currentVersion!,
            targetSource: candidate.source,
          },
        };
        await f.operations.start(request);
        await vi.waitFor(async () =>
          expect(await f.ledger.read(request.operationId)).toMatchObject(
            phase === "prepare"
              ? { phase: "refused", code: "install-refused" }
              : { phase: "running", code: "launcher-unconfirmed" },
          ),
        );
        expect(f.updater.admissionHeld()).toBe(phase === "handoff");
        await f.operations.start(request);
        expect(f.install).toHaveBeenCalledOnce();
        const next = { ...request, operationId: NodeCrypto.randomUUID() };
        const receipt = await f.operations.start(next);
        if (phase === "prepare") {
          expect(receipt.code).not.toBe("busy");
          await vi.waitFor(async () =>
            expect(await f.ledger.read(next.operationId)).toMatchObject({
              phase: "refused",
              code: "install-refused",
            }),
          );
          expect(f.install).toHaveBeenCalledTimes(2);
        } else {
          expect(receipt.code).toBe("busy");
          expect(f.install).toHaveBeenCalledOnce();
        }
      },
      false,
      async (_request, _step, onPreparationRefused) => {
        if (phase === "prepare") onPreparationRefused?.();
        throw new Error("Synthetic install boundary failure.");
      },
    ),
);

it("creates no idle or standing-hold sweep jobs, then coalesces release into one cleanup", async () =>
  fixture(async (f) => {
    const jobId = "66666666-6666-4666-8666-666666666666";
    const lifecycle = preparationLifecycle(f.base, jobId);
    const launch = await lifecycle.reserve("stage");
    const pid = 54321;
    await launch.started({ pid, processGroup: pid });
    await lifecycle.effects(pid, "transfer");
    const scratch = NodePath.join(preparationJobsDirectory(f.base), jobId);
    await NodeFSP.mkdir(scratch, { recursive: true });
    const kill = process.kill.bind(process);
    let live = true;
    const probe = vi.spyOn(process, "kill").mockImplementation((requested, signal) => {
      if (signal === 0 && Math.abs(requested) === pid) {
        if (live) return true;
        throw Object.assign(new Error("Synthetic group absence."), { code: "ESRCH" });
      }
      return kill(requested, signal);
    });
    const preparation = makePreparationSupervisor({
      baseDir: f.base,
      cacheRoot: NodePath.join(f.base, "cache"),
      command: sourceWorkerCommand,
    });
    // The worker must see a deterministic standing hold too, independently of the parent probe.
    const hold = () =>
      withQualifiedRuntimeLock(f.base, preparationJobLock(jobId), async () => {
        const operations = createTargetOperations({
          key: f.base,
          ledger: f.ledger,
          fleetStore: f.fleetStore,
          updater: f.updater,
          preparation,
          refreshPolicy: f.refreshPolicy,
          install: f.install,
          reconcileNative: f.reconcileNative,
          onFailure: () => {},
        });
        await operations.sweep(); // Startup demand, despite the hold.
        const history = NodePath.join(f.base, "runtime", "jones-preparation", "lifecycle");
        const names = async () =>
          (await NodeFSP.readdir(history))
            .filter((name) => !name.endsWith(".effects.json"))
            .toSorted();
        const locks = async () =>
          (await NodeFSP.readdir(NodePath.join(f.base, "runtime", "jones-preparation")))
            .filter((name) => name.startsWith("job-owner-"))
            .toSorted();
        const first = await names();
        const firstLocks = await locks();
        expect(first).toHaveLength(2); // Held job and exactly one startup sweep.
        for (let tick = 0; tick < 20; tick++) await operations.sweep();
        expect(await names()).toEqual(first);
        expect(await locks()).toEqual(firstLocks);
        expect(f.updater.admissionHeld()).toBe(true);
        return { operations, names, locks };
      });
    try {
      const { operations, names, locks } = await hold();
      live = false;
      await Promise.all([operations.sweep(), operations.sweep(), operations.sweep()]);
      expect(await names()).toHaveLength(3);
      expect(await locks()).toHaveLength(3);
      expect(f.updater.admissionHeld()).toBe(false);
      await expect(NodeFSP.stat(scratch)).rejects.toMatchObject({ code: "ENOENT" });
      const idle = await names();
      for (let tick = 0; tick < 20; tick++) await operations.sweep();
      expect(await names()).toEqual(idle);
    } finally {
      probe.mockRestore();
    }
  }));

it("preserves holds and cleanup demand when reconciliation fails", async () =>
  fixture(async (f) => {
    let fail = false;
    let held = ["77777777-7777-4777-8777-777777777777"];
    const requestSweep = vi.fn();
    const sweepIfNeeded = vi.fn(async () => {});
    const preparation: PreparationSupervisor = {
      ...f.preparation,
      unreleasedJobs: async () => {
        if (fail) throw new Error("Synthetic lifecycle read failure.");
        return held;
      },
      requestSweep,
      sweepIfNeeded,
    };
    const operations = createTargetOperations({
      key: f.base,
      ledger: f.ledger,
      fleetStore: f.fleetStore,
      updater: f.updater,
      preparation,
      refreshPolicy: f.refreshPolicy,
      install: f.install,
      reconcileNative: f.reconcileNative,
      onFailure: () => {},
    });
    await operations.reconcile();
    fail = true;
    await expect(operations.sweep()).rejects.toThrow("Synthetic lifecycle read failure.");
    expect(f.updater.admissionHeld()).toBe(true);
    expect(requestSweep).toHaveBeenCalledTimes(1);
    expect(sweepIfNeeded).not.toHaveBeenCalled();
    fail = false;
    held = [];
    await operations.sweep();
    expect(f.updater.admissionHeld()).toBe(false);
    expect(requestSweep).toHaveBeenCalledTimes(2);
    expect(sweepIfNeeded).toHaveBeenCalledTimes(1);
  }));

it("tracks newly found lifecycle holds even when the subsequent ledger scan fails", async () =>
  fixture(async (f) => {
    const id = "88888888-8888-4888-8888-888888888888";
    let unresolved = [id];
    let fail = true;
    const requestSweep = vi.fn();
    const preparation: PreparationSupervisor = {
      ...f.preparation,
      unreleasedJobs: async () => unresolved,
      requestSweep,
      sweepIfNeeded: async () => {},
    };
    const operations = createTargetOperations({
      key: f.base,
      ledger: {
        ...f.ledger,
        listUnsettled: async function* () {
          if (fail) throw new Error("Synthetic ledger read failure.");
          yield* f.ledger.listUnsettled();
        },
      },
      fleetStore: f.fleetStore,
      updater: f.updater,
      preparation,
      refreshPolicy: f.refreshPolicy,
      install: f.install,
      reconcileNative: f.reconcileNative,
      onFailure: () => {},
    });
    await expect(operations.reconcile()).rejects.toThrow("Synthetic ledger read failure.");
    expect(f.updater.admissionHeld()).toBe(true);
    unresolved = [];
    fail = false;
    await operations.sweep();
    expect(f.updater.admissionHeld()).toBe(false);
    expect(requestSweep).toHaveBeenCalledTimes(2);
  }));
