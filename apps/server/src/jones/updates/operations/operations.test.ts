// @effect-diagnostics nodeBuiltinImport:off -- Fixtures own only their unique synthetic homes.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";
import { describe, it, expect, vi } from "vite-plus/test";
import { EnvironmentId, type JonesUpdateState } from "@t3tools/contracts";
import type {
  JonesOperationReceipt,
  JonesOperationStartInput,
} from "@t3tools/contracts/jones/jonesOperations";
import type {
  JonesActionsCandidate,
  JonesActionsCheckResult,
} from "@t3tools/shared/jones/jonesActions";
import { JonesUpdater, type JonesPreparedStage } from "../JonesUpdater.ts";
import type { OperationReconciliation } from "../launcherOperation.ts";
import {
  makePreparationSupervisor,
  type PreparationJoin,
  type PreparationSupervisor,
} from "../preparation/supervisor.ts";
import { preparationLifecycle } from "../preparation/lifecycle.ts";
import { preparationJobsDirectory } from "../preparation/protocol.ts";
import { sweepPreparationLeftovers } from "../preparation/sweep.ts";
import { createOperationLedger } from "./ledger.ts";
import { createTargetOperations } from "./operations.ts";
import { createRecoveryPreflights } from "./recoveryPreflight.ts";

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
async function fixture(
  run: (f: Awaited<ReturnType<typeof make>>) => Promise<void>,
  recoveryEnabled = false,
) {
  const base = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "jones-target-operation-"));
  try {
    await run(await make(base, recoveryEnabled));
  } finally {
    await NodeFSP.rm(base, { recursive: true, force: true });
  }
}
async function make(base: string, recoveryEnabled: boolean) {
  const joins = new Map<string, PreparationJoin | "running">();
  let removedJobIds: string[] = [];
  const preparation: PreparationSupervisor = {
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
  let publicationBarrier: Promise<void> | undefined;
  const checkEntered = deferred<void>();
  const admissionReserved = deferred<void>();
  const prepareStarted = deferred<void>();
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
        prepareCalls++;
        jobId = options.jobId!;
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
      install: async () => {},
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
  const operations = createTargetOperations({
    key: base,
    ledger,
    updater,
    preparation,
    refreshPolicy,
    install,
    reconcileNative: async () => native,
    onFailure: () => {},
    ...(recoveryEnabled
      ? { recovery: { validate: recovery.validate, observed: async () => "clone" as const } }
      : {}),
  });
  const receipt = () => ledger.read(input.operationId);
  return {
    base,
    ledger,
    operations,
    updater,
    install,
    receipt,
    check,
    checkEntered: checkEntered.promise,
    admissionReserved: admissionReserved.promise,
    prepareStarted: prepareStarted.promise,
    refreshPolicy,
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
    ["operations", "unsupported"],
    ["download", "unsupported"],
    ["check", "unsupported"],
    ["exact source", "unsupported"],
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
      await f.operations.stop({ ...input, stopId: NodeCrypto.randomUUID() });
      check.resolve();
      await vi.waitFor(async () => expect((await f.receipt())?.phase).toBe("stopped"));
      expect(f.prepareCalls()).toBe(0);
    }));
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
  it("holds a restarted download whose worker release is unknown", async () =>
    fixture(async (f) => {
      await f.ledger.accept(input);
      await f.ledger.transition(input.operationId, (r) => ({
        ...r,
        phase: "running",
        jobId: NodeCrypto.randomUUID(),
      }));
      await f.operations.reconcile();
      expect(await f.receipt()).toMatchObject({ phase: "held", code: "unreaped" });
      expect(f.updater.admissionHeld()).toBe(true);
      expect(
        (
          await f.operations.read({
            environmentId: input.environmentId,
            operationIds: [NodeCrypto.randomUUID()],
          })
        ).receipts[0]?.receipt,
      ).toBeNull();
    }));
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
        await lifecycle.reserve("stage");
        await lifecycle.launch({
          kind: "stage",
          state: "started",
          worker: { pid, processGroup: pid },
        });
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
