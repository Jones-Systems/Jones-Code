// @effect-diagnostics globalDate:off - Fixed timestamps and injected clocks exercise receipt, lease and review behavior.
import { expect, it } from "vite-plus/test";
import { EnvironmentId } from "@t3tools/contracts";
import type { JonesInstallPlanState } from "@t3tools/contracts/jones/installPlan";
import type { JonesRecoveryPreflight } from "@t3tools/contracts/jones/jonesOperations";
import { startPlan } from "./model.ts";
import { installPlanPresentation, recoveryPreflightPresentation } from "./presentation.ts";
import type { JonesUpdateState } from "@t3tools/contracts/jones/jonesUpdates";

const now = "2026-10-27T05:00:00Z",
  source = "a".repeat(40),
  tree = "b".repeat(40);
const env = EnvironmentId.make("laptop"),
  planId = "11111111-1111-4111-8111-111111111111";

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
