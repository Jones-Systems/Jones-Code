// @effect-diagnostics globalDate:off - Fixed timestamps and injected clocks exercise receipt, lease and review behavior.
import { expect, it } from "vite-plus/test";
import { EnvironmentId } from "@t3tools/contracts";
import type { JonesInstallPlanState } from "@t3tools/contracts/jones/installPlan";
import type {
  JonesOperationReceipt,
  JonesRecoveryPreflight,
} from "@t3tools/contracts/jones/jonesOperations";
import { startPlan } from "./model.ts";
import { installPlanPresentation, recoveryPreflightPresentation } from "./presentation.ts";
import type { JonesUpdateState } from "@t3tools/contracts/jones/jonesUpdates";

const now = "2026-10-27T05:00:00Z",
  source = "a".repeat(40),
  tree = "b".repeat(40);
const env = EnvironmentId.make("laptop"),
  planId = "11111111-1111-4111-8111-111111111111";

it.each(["refused", "failed"] as const)(
  "offers an explicit Retry download for a %s receipt only with fresh permission",
  (phase) => {
    const host = EnvironmentId.make("mini");
    const receipt: JonesOperationReceipt = {
      protocol: 1,
      operationId: planId,
      environmentId: host,
      expectedInstalledSource: "c".repeat(40),
      action: { kind: "download-exact", targetSource: source },
      phase,
      code: phase === "refused" ? "busy" : "download-failed",
      ...(phase === "failed" ? { acceptedAt: now } : {}),
      updatedAt: now,
    };
    const base = startPlan({
      planId,
      now,
      origin: "update-button",
      laptopEnvironmentId: env,
      select: [{ environmentId: host, label: "Mini", route: "operations" }],
      target: {
        repository: "Jones-Systems/Jones-Code",
        sourceSha: source,
        sourceTree: tree,
        version: "new",
      },
      from: { version: "old" },
    });
    const state: JonesInstallPlanState = {
      schema: 1,
      current: {
        ...base,
        laptop: {
          ...base.laptop,
          gate: {
            openedAt: now,
            transactionId: "tx",
            activeGeneration: "gen",
            basis: "plan-install",
            activeSource: source,
            activeTree: tree,
          },
        },
        devices: [
          {
            ...base.devices[0]!,
            operations: [
              {
                operationId: planId,
                step: "download",
                lease: { claimant: "fixture", reservedAt: now, leaseUntil: now },
                request: { kind: "operation-start", input: receipt },
                receipt,
              },
            ],
          },
        ],
      },
      last: null,
      laptop: { observedAt: now, activationBlocked: false },
      view: { complete: false },
    };
    const live: JonesUpdateState = {
      source: "jones-actions",
      channel: "jones-main",
      phase: "available",
      environmentId: host,
      installedSource: {
        repository: "Jones-Systems/Jones-Code",
        sha: receipt.expectedInstalledSource,
        tree,
      },
      capability: { check: true, download: true, install: true, exactSource: true, operations: 1 },
      operation: receipt,
    };
    const observe = (
      value = live,
      freshness: "fresh" | "stale" = "fresh",
      canOperate = true,
    ) =>
      installPlanPresentation(state, {
        now: Date.parse(now),
        devices: new Map([[host, { observation: { state: value, freshness }, canOperate }]]),
      }).rows[0]!;
    expect(observe()).toMatchObject({
      phase: "not-updated",
      downloadLabel: "Retry download",
      actions: { download: true, install: false, checkAgain: true },
    });
    if (phase === "refused") expect(observe().text).toContain("Download request did not start");
    expect(observe(live, "stale").actions.download).toBe(false);
    expect(observe(live, "fresh", false).actions.download).toBe(false);
    for (const operation of [
      { ...receipt, phase: "held" as const, code: "unreaped" },
      { ...receipt, phase: "running" as const, code: "launcher-unconfirmed" },
    ])
      expect(observe({ ...live, operation }).actions.download).toBe(false);
    expect(state.current?.devices[0]?.operations).toHaveLength(1);
    const laptop = installPlanPresentation(
      {
        ...state,
        current: {
          ...base,
          laptop: {
            ...base.laptop,
            operations: [
              {
                operationId: planId,
                step: "download",
                requestedAt: now,
                result: { status: phase, at: now },
              },
            ],
          },
        },
      },
      {
        now: Date.parse(now),
        laptop: {
          freshness: "fresh",
          state: {
            ...live,
            environmentId: env,
            provenance: {
              repository: "Jones-Systems/Jones-Code",
              sourceSha: source,
              sourceTree: tree,
              workflow: "fixture",
              runId: 1,
              runAttempt: 1,
              artifactId: 1,
              artifactDigest: "c".repeat(64),
              platform: "darwin",
              architecture: "arm64",
            },
          },
        },
      },
    ).laptop;
    expect(laptop).toMatchObject({
      phase,
      downloadLabel: "Retry download",
      actions: { download: true, checkAgain: true },
    });
    if (phase === "refused") expect(laptop?.text).toBe("Download request did not start");
  },
);

it("offers reviewed install for a fresh matching cold Laptop stage without opening the gate", () => {
  const plan = startPlan({
    planId,
    now,
    origin: "update-button",
    laptopEnvironmentId: env,
    select: [],
    target: {
      repository: "Jones-Systems/Jones-Code",
      sourceSha: source,
      sourceTree: tree,
      version: "new",
    },
    from: { version: "old" },
  });
  const state: JonesInstallPlanState = {
    schema: 1,
    current: plan,
    last: null,
    laptop: { observedAt: now, activationBlocked: false },
    view: { complete: false },
  };
  const staged: JonesUpdateState = {
    source: "jones-actions",
    channel: "jones-main",
    phase: "staged",
    revision: 1,
    capability: { check: true, download: true, install: true },
    currentVersion: "old",
    stagedHandle: "cold-stage",
    provenance: {
      repository: "Jones-Systems/Jones-Code",
      sourceSha: source,
      sourceTree: tree,
      workflow: "fixture",
      runId: 1,
      runAttempt: 1,
      artifactId: 1,
      artifactDigest: "c".repeat(64),
      platform: "darwin",
      architecture: "arm64",
    },
  };
  const observe = (native = state, value = staged, freshness: "fresh" | "loading" = "fresh") =>
    installPlanPresentation(native, { now: Date.parse(now), laptop: { state: value, freshness } });
  expect(observe()).toMatchObject({
    complete: false,
    headerDoneVisible: false,
    laptop: {
      phase: "staged",
      text: "Downloaded · ready to install",
      actions: { install: true, download: false, stop: false },
    },
  });
  expect(plan.laptop.operations).toEqual([]);
  expect(plan.laptop.gate).toBeUndefined();
  const { stagedHandle, ...withoutHandle } = staged;
  expect(stagedHandle).toBe("cold-stage");
  for (const value of [
    { ...staged, provenance: { ...staged.provenance!, sourceSha: "d".repeat(40) } },
    { ...staged, provenance: { ...staged.provenance!, sourceTree: "d".repeat(40) } },
    withoutHandle,
    { ...staged, capability: { ...staged.capability, install: false } },
  ])
    expect(observe(state, value).laptop?.actions.install).toBe(false);
  expect(observe(state, staged, "loading").laptop?.actions.install).toBe(false);
  expect(
    observe({ ...state, readOnly: { code: "invalid", message: "Held" } }).laptop?.actions.install,
  ).toBe(false);
  const held = {
    ...state,
    current: {
      ...plan,
      laptop: {
        ...plan.laptop,
        operations: [
          {
            operationId: planId,
            step: "install" as const,
            requestedAt: now,
            result: { status: "held" as const, at: now },
          },
        ],
      },
    },
  };
  expect(observe(held)).toMatchObject({
    complete: false,
    headerDoneVisible: false,
    laptop: { phase: "held", actions: { install: false } },
  });
});
it("shows completion Done only for a current complete native plan and keeps read-only reopen inert", () => {
  const plan = startPlan({
    planId,
    now,
    origin: "update-button",
    laptopEnvironmentId: env,
    select: [],
    target: {
      repository: "Jones-Systems/Jones-Code",
      sourceSha: source,
      sourceTree: tree,
      version: "new",
    },
    from: { version: "old" },
  });
  const state: JonesInstallPlanState = {
    schema: 1,
    current: plan,
    last: null,
    laptop: { observedAt: now, activationBlocked: false },
    view: { complete: true },
  };
  expect(installPlanPresentation(state, { now: Date.parse(now) }).headerDoneVisible).toBe(false);
  const complete = {
    ...plan,
    laptop: {
      ...plan.laptop,
      gate: {
        openedAt: now,
        transactionId: "tx",
        activeGeneration: "gen",
        basis: "plan-install" as const,
        activeSource: source,
        activeTree: tree,
      },
    },
  };
  expect(
    installPlanPresentation({ ...state, current: complete }, { now: Date.parse(now) })
      .headerDoneVisible,
  ).toBe(true);
  expect(
    installPlanPresentation({ ...state, current: null, last: complete }, { now: Date.parse(now) }),
  ).toMatchObject({ readOnly: true, headerDoneVisible: false });
  expect(
    installPlanPresentation(
      { ...state, current: complete, readOnly: { code: "invalid", message: "Invalid plan" } },
      { now: Date.parse(now) },
    ).headerDoneVisible,
  ).toBe(false);
});

it("offers Clone only with proof and capacity, and claims no backup for a first install", () => {
  const preflight: JonesRecoveryPreflight = {
    protocol: 1,
    preflightId: planId,
    measuredAt: now,
    expiresAt: now,
    binding: {
      environmentId: env,
      stagedHandle: "stage",
      currentVersion: "old",
      expectedInstalledSource: tree,
    },
    firstInstall: true,
    stateBytes: 0,
    volumes: [],
    clone: { status: "unknown", reason: "Clone not proved" },
    policies: {
      cloneOrCopy: { admissible: false, reason: "Low copy space" },
      cloneRequired: { admissible: true },
    },
  };
  expect(recoveryPreflightPresentation(preflight, "Laptop")).toMatchObject({
    clone: false,
    cloneOrCopy: false,
    stop: true,
    text: "First install on Laptop · no existing data to back up",
  });
  expect(
    recoveryPreflightPresentation(
      { ...preflight, firstInstall: false, clone: { status: "proved" } },
      "Host",
    ),
  ).toMatchObject({ clone: true, cloneOrCopy: false, stop: true });
});
