// @effect-diagnostics globalDate:off - Fixed timestamps and injected clocks exercise receipt, lease and review behavior.
import { describe, expect, it, vi } from "vite-plus/test";
import { EnvironmentId } from "@t3tools/contracts";
import type { JonesUpdateState } from "@t3tools/contracts/jones/jonesUpdates";
import type {
  JonesOperationReceipt,
  JonesRecoveryPreflight,
} from "@t3tools/contracts/jones/jonesOperations";
import {
  executeJonesAction,
  isNativeJonesInstallReview,
  prepareReviewedInstall,
  resolveDownloadAction,
  type JonesExecutorHost,
  type JonesExecutorSnapshot,
} from "./executor.ts";

const env = EnvironmentId.make("executor-host");
const source = "a".repeat(40),
  installed = "b".repeat(40),
  tree = "c".repeat(40);
const operationId = "11111111-1111-4111-8111-111111111111";
const now = Date.parse("2026-10-27T05:00:00.000Z");
function fixture(staged = false, legacyHost = false) {
  let state: JonesUpdateState = {
    source: "jones-actions",
    channel: "jones-main",
    phase: staged ? "staged" : "available",
    environmentId: env,
    currentVersion: "old",
    ...(legacyHost
      ? {}
      : {
          installedSource: {
            repository: "Jones-Systems/Jones-Code" as const,
            sha: installed,
            tree,
          },
        }),
    capability: legacyHost
      ? { check: true, download: true, install: true }
      : { check: true, download: true, install: true, exactSource: true, operations: 1 },
    ...(staged ? { stagedHandle: "stage" } : {}),
    provenance: {
      repository: "Jones-Systems/Jones-Code",
      sourceSha: source,
      sourceTree: tree,
      workflow: "linux",
      runId: 1,
      runAttempt: 1,
      artifactId: 2,
      artifactDigest: "d".repeat(64),
      platform: "linux",
      architecture: "x64",
      version: "new",
    },
  };
  const readLatest = vi.fn<JonesExecutorHost["readLatest"]>(() => ({
    observation: { freshness: "fresh", state },
    canOperate: true,
  }));
  const start = vi.fn<JonesExecutorHost["start"]>(
    async (input): Promise<JonesOperationReceipt> => ({
      ...input,
      protocol: 1,
      phase: "accepted",
      acceptedAt: new Date(now).toISOString(),
      updatedAt: new Date(now).toISOString(),
    }),
  );
  const legacy = vi.fn<JonesExecutorHost["legacy"]>(async (request) => ({
    ...state,
    phase: request.action === "install" ? "installing" : "downloading",
  }));
  const confirm = vi.fn<JonesExecutorHost["confirm"]>(async () => true);
  const host = { readLatest, start, legacy, confirm };
  return {
    host,
    state: () => state,
    setState: (next: JonesUpdateState) => {
      state = next;
    },
  };
}
const download = { kind: "download-exact" as const, targetSource: source };

function recoveryPreflightFixture(): JonesRecoveryPreflight {
  return {
    protocol: 1,
    preflightId: operationId,
    measuredAt: new Date(now).toISOString(),
    expiresAt: new Date(now + 300000).toISOString(),
    binding: {
      environmentId: env,
      stagedHandle: "stage",
      currentVersion: "old",
      expectedInstalledSource: installed,
    },
    firstInstall: false,
    stateBytes: 100,
    volumes: [],
    clone: { status: "proved" },
    policies: {
      cloneOrCopy: { admissible: false, reason: "Low copy space" },
      cloneRequired: { admissible: true },
    },
  };
}

const legacyBindingChanges: ReadonlyArray<{
  name: string;
  change: (snapshot: JonesExecutorSnapshot) => JonesExecutorSnapshot;
}> = [
  {
    name: "environment",
    change: (snapshot) => ({
      ...snapshot,
      observation: {
        ...snapshot.observation,
        state: { ...snapshot.observation.state!, environmentId: EnvironmentId.make("other-host") },
      },
    }),
  },
  {
    name: "stage",
    change: (snapshot) => ({
      ...snapshot,
      observation: {
        ...snapshot.observation,
        state: { ...snapshot.observation.state!, stagedHandle: "another-stage" },
      },
    }),
  },
  {
    name: "version",
    change: (snapshot) => ({
      ...snapshot,
      observation: {
        ...snapshot.observation,
        state: { ...snapshot.observation.state!, currentVersion: "another-version" },
      },
    }),
  },
  {
    name: "target source",
    change: (snapshot) => ({
      ...snapshot,
      observation: {
        ...snapshot.observation,
        state: {
          ...snapshot.observation.state!,
          provenance: { ...snapshot.observation.state!.provenance!, sourceSha: tree },
        },
      },
    }),
  },
  {
    name: "newly reported installed source",
    change: (snapshot) => ({
      ...snapshot,
      observation: {
        ...snapshot.observation,
        state: {
          ...snapshot.observation.state!,
          installedSource: { repository: "Jones-Systems/Jones-Code", sha: installed, tree },
        },
      },
    }),
  },
  {
    name: "permission",
    change: (snapshot) => ({ ...snapshot, canOperate: false }),
  },
  {
    name: "freshness",
    change: (snapshot) => ({
      ...snapshot,
      observation: { ...snapshot.observation, freshness: "stale" },
    }),
  },
  {
    name: "install capability",
    change: (snapshot) => ({
      ...snapshot,
      observation: {
        ...snapshot.observation,
        state: {
          ...snapshot.observation.state!,
          capability: { ...snapshot.observation.state!.capability, install: false },
        },
      },
    }),
  },
];

describe("shared Jones action execution", () => {
  it("keeps exact selection and matching legacy artifact fallback, and never turns Download into Install", () => {
    const f = fixture();
    const input = { environmentId: env, targetSource: source, ...f.host.readLatest() };
    expect(resolveDownloadAction(input)).toEqual({ kind: "ready", action: download });
    f.setState({ ...f.state(), capability: { check: true, download: true, install: true } });
    expect(resolveDownloadAction({ ...input, ...f.host.readLatest() })).toEqual({
      kind: "ready",
      action: { kind: "download", artifactId: 2, sourceSha: source },
    });
    f.setState({ ...f.state(), phase: "no-new" });
    expect(resolveDownloadAction({ ...input, ...f.host.readLatest() }).kind).toBe("refused");
    const stage = fixture(true);
    expect(resolveDownloadAction({ ...input, ...stage.host.readLatest() }).kind).toBe("refused");
  });

  it("does not bypass a native plan handoff, and refuses stale membership grants", async () => {
    const f = fixture();
    const host: JonesExecutorHost = {
      ...f.host,
      planGate: async () => ({ kind: "handled", message: "Added to the update plan." }),
    };
    expect(
      await executeJonesAction({
        environmentId: env,
        targetSource: source,
        action: download,
        operationId,
        host,
      }),
    ).toMatchObject({ kind: "accepted" });
    expect(f.host.start).not.toHaveBeenCalled();
    expect(f.host.legacy).not.toHaveBeenCalled();
    const gated: JonesExecutorHost = {
      ...f.host,
      planGate: async () => ({ kind: "refused", message: "Laptop is not verified." }),
    };
    expect(
      await executeJonesAction({
        environmentId: env,
        targetSource: source,
        action: download,
        operationId,
        host: gated,
      }),
    ).toMatchObject({ kind: "refused", code: "plan-gate" });
    expect(f.host.start).not.toHaveBeenCalled();
  });

  it("uses durable IDs, treats cancellation after possible send as unknown and refuses a second request", async () => {
    const f = fixture();
    f.host.start.mockRejectedValue(new Error("lost reply"));
    const input = {
      environmentId: env,
      targetSource: source,
      action: download,
      operationId,
      host: f.host,
    };
    expect(await executeJonesAction(input)).toMatchObject({ kind: "unknown" });
    f.setState({ ...f.state(), revision: 0 });
    expect(await executeJonesAction({ ...input, unresolved: true })).toMatchObject({
      kind: "unknown",
      code: "unresolved",
    });
    expect(f.host.start).toHaveBeenCalledTimes(1);
    expect(f.host.start.mock.calls[0]?.[0]).toMatchObject({
      operationId,
      expectedInstalledSource: installed,
      action: download,
    });
    expect(await executeJonesAction({ ...input, cancelled: () => true })).toMatchObject({
      kind: "refused",
      code: "cancelled",
    });
  });

  it("retains lost legacy download results as unknown and never retries through another endpoint", async () => {
    const f = fixture();
    f.setState({
      ...f.state(),
      capability: { check: true, download: true, install: true, exactSource: true },
    });
    f.host.legacy.mockRejectedValue(new Error("lost reply"));
    expect(
      await executeJonesAction({
        environmentId: env,
        targetSource: source,
        action: download,
        operationId,
        host: f.host,
      }),
    ).toMatchObject({ kind: "unknown" });
    expect(f.host.legacy).toHaveBeenCalledTimes(1);
    expect(f.host.start).not.toHaveBeenCalled();
  });

  it("does not treat a legacy phase or revision reset as settlement of an outstanding receipt", async () => {
    const f = fixture();
    f.setState({
      ...f.state(),
      revision: 0,
      phase: "available",
      operation: {
        protocol: 1,
        operationId,
        environmentId: env,
        expectedInstalledSource: installed,
        action: download,
        phase: "held",
        code: "unreaped",
        acceptedAt: new Date(now).toISOString(),
        updatedAt: new Date(now).toISOString(),
      },
    });
    expect(
      await executeJonesAction({
        environmentId: env,
        targetSource: source,
        action: download,
        operationId,
        host: f.host,
      }),
    ).toMatchObject({ kind: "unknown", code: "outstanding-operation" });
    expect(f.host.start).not.toHaveBeenCalled();
    expect(f.host.legacy).not.toHaveBeenCalled();
  });

  it("cancels review before dispatch and refuses a changed stage or source after confirmation", async () => {
    const f = fixture(true);
    f.host.confirm.mockResolvedValue(false);
    const input = {
      environmentId: env,
      targetSource: source,
      label: "Host",
      host: f.host,
      now: () => now,
    };
    expect(await prepareReviewedInstall(input)).toMatchObject({
      kind: "refused",
      code: "cancelled",
    });
    f.host.confirm.mockImplementation(async () => {
      f.setState({ ...f.state(), stagedHandle: "different" });
      return true;
    });
    expect(await prepareReviewedInstall(input)).toMatchObject({ kind: "refused", code: "changed" });
    expect(f.host.start).not.toHaveBeenCalled();
    expect(f.host.legacy).not.toHaveBeenCalled();
  });

  it("requires a reviewed install, then revalidates current identity before operations/start", async () => {
    const f = fixture(true);
    const prepared = await prepareReviewedInstall({
      environmentId: env,
      targetSource: source,
      label: "Host",
      host: f.host,
      now: () => now,
    });
    if (prepared.kind !== "ready") throw new Error("Expected review");
    expect(isNativeJonesInstallReview(prepared.review)).toBe(true);
    expect(prepared.review).toMatchObject({ expectedInstalledSource: installed });
    expect(
      await executeJonesAction({
        environmentId: env,
        targetSource: source,
        operationId,
        action: prepared.action,
        host: f.host,
      }),
    ).toMatchObject({ kind: "refused", code: "review-required" });
    expect(
      await executeJonesAction({
        environmentId: env,
        targetSource: source,
        operationId,
        ...prepared,
        host: f.host,
      }),
    ).toMatchObject({ kind: "accepted" });
    expect(f.host.start.mock.calls[0]?.[0].action.kind).toBe("install");
    f.setState({ ...f.state(), installedSource: { ...f.state().installedSource!, sha: source } });
    expect(
      await executeJonesAction({
        environmentId: env,
        targetSource: source,
        operationId,
        ...prepared,
        host: f.host,
      }),
    ).toMatchObject({ kind: "refused", code: "review-required" });
    expect(f.host.start).toHaveBeenCalledTimes(1);
  });

  it.each([undefined, false, true])(
    "installs a reviewed legacy stage without installedSource (continueRunningThreads=%s)",
    async (continueRunningThreads) => {
      const f = fixture(true, true);
      const preflight = vi.fn<NonNullable<JonesExecutorHost["preflight"]>>();
      const chooseRecovery = vi.fn<NonNullable<JonesExecutorHost["chooseRecovery"]>>();
      const host = { ...f.host, preflight, chooseRecovery };
      const prepared = await prepareReviewedInstall({
        environmentId: env,
        targetSource: source,
        label: "Legacy host",
        host,
        now: () => now,
        ...(continueRunningThreads === undefined ? {} : { continueRunningThreads }),
      });
      if (prepared.kind !== "ready") throw new Error("Expected legacy review");
      expect(isNativeJonesInstallReview(prepared.review)).toBe(false);
      expect(prepared.review).toEqual({
        kind: "legacy-staged",
        environmentId: env,
        reviewedAt: new Date(now).toISOString(),
        stagedHandle: "stage",
        targetSource: source,
        currentVersion: "old",
        ...(continueRunningThreads === undefined ? {} : { continueRunningThreads }),
      });
      expect(
        await executeJonesAction({
          environmentId: env,
          targetSource: source,
          operationId,
          ...prepared,
          host,
        }),
      ).toMatchObject({ kind: "accepted", state: { phase: "installing" } });
      expect(f.host.confirm).toHaveBeenCalledTimes(1);
      expect(f.host.legacy).toHaveBeenCalledExactlyOnceWith({
        action: "install",
        input: {
          environmentId: env,
          stagedHandle: "stage",
          currentVersion: "old",
          ...(continueRunningThreads === undefined ? {} : { continueRunningThreads }),
        },
      });
      expect(f.host.start).not.toHaveBeenCalled();
      expect(preflight).not.toHaveBeenCalled();
      expect(chooseRecovery).not.toHaveBeenCalled();
    },
  );

  it.each(legacyBindingChanges)(
    "refuses a legacy $name change during confirmation",
    async ({ change }) => {
      const f = fixture(true, true);
      f.host.confirm.mockImplementation(async () => {
        f.host.readLatest.mockReturnValue(change(f.host.readLatest()));
        return true;
      });
      expect(
        await prepareReviewedInstall({
          environmentId: env,
          targetSource: source,
          label: "Legacy host",
          host: f.host,
          now: () => now,
        }),
      ).toMatchObject({ kind: "refused", code: "changed" });
      expect(f.host.confirm).toHaveBeenCalledTimes(1);
      expect(f.host.legacy).not.toHaveBeenCalled();
      expect(f.host.start).not.toHaveBeenCalled();
    },
  );

  it.each(legacyBindingChanges)(
    "refuses a legacy $name change after review",
    async ({ change }) => {
      const f = fixture(true, true);
      const prepared = await prepareReviewedInstall({
        environmentId: env,
        targetSource: source,
        label: "Legacy host",
        host: f.host,
        now: () => now,
      });
      if (prepared.kind !== "ready") throw new Error("Expected legacy review");
      f.host.readLatest.mockReturnValue(change(f.host.readLatest()));
      expect(
        await executeJonesAction({
          environmentId: env,
          targetSource: source,
          operationId,
          ...prepared,
          host: f.host,
        }),
      ).toMatchObject({ kind: "refused" });
      expect(f.host.legacy).not.toHaveBeenCalled();
      expect(f.host.start).not.toHaveBeenCalled();
    },
  );

  it("cannot reuse a legacy review for another host with the same stage and version", async () => {
    const f = fixture(true, true);
    const prepared = await prepareReviewedInstall({
      environmentId: env,
      targetSource: source,
      label: "Legacy host",
      host: f.host,
      now: () => now,
    });
    if (prepared.kind !== "ready") throw new Error("Expected legacy review");
    const other = EnvironmentId.make("other-host");
    f.setState({ ...f.state(), environmentId: other });
    expect(
      await executeJonesAction({
        environmentId: other,
        targetSource: source,
        operationId,
        ...prepared,
        host: f.host,
      }),
    ).toMatchObject({ kind: "refused", code: "review-required" });
    expect(f.host.legacy).not.toHaveBeenCalled();
    expect(f.host.start).not.toHaveBeenCalled();
  });

  it.each(["operations", "recoveryPolicy", "planGate"] as const)(
    "requires installedSource before reviewing a %s route",
    async (route) => {
      const f = fixture(true, true);
      const planGate = vi.fn<NonNullable<JonesExecutorHost["planGate"]>>();
      const preflight = vi.fn<NonNullable<JonesExecutorHost["preflight"]>>();
      const chooseRecovery = vi.fn<NonNullable<JonesExecutorHost["chooseRecovery"]>>();
      if (route !== "planGate")
        f.setState({ ...f.state(), capability: { ...f.state().capability, [route]: 1 } });
      const host = {
        ...f.host,
        preflight,
        chooseRecovery,
        ...(route === "planGate" ? { planGate } : {}),
      };
      expect(
        await prepareReviewedInstall({
          environmentId: env,
          targetSource: source,
          label: "Host",
          host,
          now: () => now,
        }),
      ).toMatchObject({ kind: "refused", code: "changed" });
      expect(f.host.confirm).not.toHaveBeenCalled();
      expect(f.host.start).not.toHaveBeenCalled();
      expect(f.host.legacy).not.toHaveBeenCalled();
      expect(preflight).not.toHaveBeenCalled();
      expect(chooseRecovery).not.toHaveBeenCalled();
      expect(planGate).not.toHaveBeenCalled();
    },
  );

  it.each(["operations", "recoveryPolicy", "planGate"] as const)(
    "refuses a legacy route upgrade to %s during confirmation",
    async (route) => {
      const f = fixture(true, true);
      const planGate = vi.fn<NonNullable<JonesExecutorHost["planGate"]>>();
      const preflight = vi.fn<NonNullable<JonesExecutorHost["preflight"]>>();
      const chooseRecovery = vi.fn<NonNullable<JonesExecutorHost["chooseRecovery"]>>();
      const host: JonesExecutorHost = { ...f.host, preflight, chooseRecovery };
      f.host.confirm.mockImplementation(async () => {
        if (route === "planGate") Object.assign(host, { planGate });
        else f.setState({ ...f.state(), capability: { ...f.state().capability, [route]: 1 } });
        return true;
      });
      expect(
        await prepareReviewedInstall({
          environmentId: env,
          targetSource: source,
          label: "Legacy host",
          host,
          now: () => now,
        }),
      ).toMatchObject({ kind: "refused", code: "changed" });
      expect(f.host.legacy).not.toHaveBeenCalled();
      expect(f.host.start).not.toHaveBeenCalled();
      expect(preflight).not.toHaveBeenCalled();
      expect(chooseRecovery).not.toHaveBeenCalled();
      expect(planGate).not.toHaveBeenCalled();
    },
  );

  it.each(["operations", "recoveryPolicy", "planGate"] as const)(
    "refuses a legacy review when a later dispatch uses %s",
    async (route) => {
      const f = fixture(true, true);
      const prepared = await prepareReviewedInstall({
        environmentId: env,
        targetSource: source,
        label: "Legacy host",
        host: f.host,
        now: () => now,
      });
      if (prepared.kind !== "ready") throw new Error("Expected legacy review");
      const planGate = vi.fn<NonNullable<JonesExecutorHost["planGate"]>>();
      if (route !== "planGate")
        f.setState({
          ...f.state(),
          installedSource: { repository: "Jones-Systems/Jones-Code", sha: installed, tree },
          capability: { ...f.state().capability, [route]: 1 },
        });
      const host = { ...f.host, ...(route === "planGate" ? { planGate } : {}) };
      expect(
        await executeJonesAction({
          environmentId: env,
          targetSource: source,
          operationId,
          ...prepared,
          host,
        }),
      ).toMatchObject({ kind: "refused", code: "review-required" });
      expect(f.host.legacy).not.toHaveBeenCalled();
      expect(f.host.start).not.toHaveBeenCalled();
      expect(planGate).not.toHaveBeenCalled();
    },
  );

  it("refuses a native operations downgrade during confirmation even with the same installed source", async () => {
    const f = fixture(true);
    f.host.confirm.mockImplementation(async () => {
      f.setState({
        ...f.state(),
        capability: { check: true, download: true, install: true },
      });
      return true;
    });
    expect(
      await prepareReviewedInstall({
        environmentId: env,
        targetSource: source,
        label: "Host",
        host: f.host,
        now: () => now,
      }),
    ).toMatchObject({ kind: "refused", code: "changed" });
    expect(f.host.confirm).toHaveBeenCalledTimes(1);
    expect(f.host.start).not.toHaveBeenCalled();
    expect(f.host.legacy).not.toHaveBeenCalled();
  });

  it("rechecks the legacy route on the dispatch snapshot", async () => {
    const f = fixture(true, true);
    const prepared = await prepareReviewedInstall({
      environmentId: env,
      targetSource: source,
      label: "Legacy host",
      host: f.host,
      now: () => now,
    });
    if (prepared.kind !== "ready") throw new Error("Expected legacy review");
    const initial = f.host.readLatest();
    const upgraded: JonesExecutorSnapshot = {
      ...initial,
      observation: {
        ...initial.observation,
        state: {
          ...f.state(),
          installedSource: { repository: "Jones-Systems/Jones-Code", sha: installed, tree },
          capability: { ...f.state().capability, operations: 1 },
        },
      },
    };
    f.host.readLatest.mockReset().mockReturnValueOnce(initial).mockReturnValue(upgraded);
    expect(
      await executeJonesAction({
        environmentId: env,
        targetSource: source,
        operationId,
        ...prepared,
        host: f.host,
      }),
    ).toMatchObject({ kind: "refused", code: "changed" });
    expect(f.host.legacy).not.toHaveBeenCalled();
    expect(f.host.start).not.toHaveBeenCalled();
  });

  it("retains a lost legacy install reply as unknown without resending", async () => {
    const f = fixture(true, true);
    const prepared = await prepareReviewedInstall({
      environmentId: env,
      targetSource: source,
      label: "Legacy host",
      host: f.host,
      now: () => now,
    });
    if (prepared.kind !== "ready") throw new Error("Expected legacy review");
    f.host.legacy.mockRejectedValue(new Error("lost install reply"));
    const input = {
      environmentId: env,
      targetSource: source,
      operationId,
      ...prepared,
      host: f.host,
    };
    expect(await executeJonesAction(input)).toMatchObject({ kind: "unknown" });
    expect(await executeJonesAction({ ...input, unresolved: true })).toMatchObject({
      kind: "unknown",
      code: "unresolved",
    });
    expect(f.host.legacy).toHaveBeenCalledTimes(1);
    expect(f.host.start).not.toHaveBeenCalled();
  });

  it.each(["preflight", "choice"] as const)(
    "refuses a native operations downgrade during recovery %s",
    async (boundary) => {
      const f = fixture(true);
      f.setState({ ...f.state(), capability: { ...f.state().capability, recoveryPolicy: 1 } });
      const downgrade = () => {
        f.setState({
          ...f.state(),
          capability: { check: true, download: true, install: true, recoveryPolicy: 1 },
        });
      };
      const preflight = vi.fn<NonNullable<JonesExecutorHost["preflight"]>>(async () => {
        if (boundary === "preflight") downgrade();
        return recoveryPreflightFixture();
      });
      const chooseRecovery = vi.fn<NonNullable<JonesExecutorHost["chooseRecovery"]>>(async () => {
        if (boundary === "choice") downgrade();
        return "clone-required";
      });
      expect(
        await prepareReviewedInstall({
          environmentId: env,
          targetSource: source,
          label: "Host",
          host: { ...f.host, preflight, chooseRecovery },
          now: () => now,
        }),
      ).toMatchObject({ kind: "refused", code: "changed" });
      expect(preflight).toHaveBeenCalledTimes(1);
      expect(chooseRecovery).toHaveBeenCalledTimes(boundary === "choice" ? 1 : 0);
      expect(f.host.start).not.toHaveBeenCalled();
      expect(f.host.legacy).not.toHaveBeenCalled();
    },
  );

  it("binds recovery choice to measured capacity, clone proof, expiry and capability", async () => {
    const f = fixture(true);
    f.setState({ ...f.state(), capability: { ...f.state().capability, recoveryPolicy: 1 } });
    const preflight = recoveryPreflightFixture();
    const host: JonesExecutorHost = {
      ...f.host,
      preflight: async () => preflight,
      chooseRecovery: async () => "clone-required",
    };
    const input = { environmentId: env, targetSource: source, label: "Host", host, now: () => now };
    expect(await prepareReviewedInstall(input)).toMatchObject({
      kind: "ready",
      review: { recovery: { policy: "clone-required", preflightId: operationId } },
    });
    expect(
      await prepareReviewedInstall({
        ...input,
        host: { ...host, preflight: async () => ({ ...preflight, clone: { status: "unknown" } }) },
      }),
    ).toMatchObject({ kind: "refused", code: "recovery-refused" });
    expect(
      await prepareReviewedInstall({
        ...input,
        host: {
          ...host,
          preflight: async () => ({ ...preflight, expiresAt: new Date(now).toISOString() }),
        },
      }),
    ).toMatchObject({ kind: "refused", code: "stale-preflight" });
    expect(f.host.start).not.toHaveBeenCalled();
  });
});
