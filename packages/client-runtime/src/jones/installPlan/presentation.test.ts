// @effect-diagnostics globalDate:off - Fixed timestamps and injected clocks exercise receipt, lease and review behavior.
import { expect, it } from "vite-plus/test";
import { EnvironmentId } from "@t3tools/contracts";
import type { JonesInstallPlanState } from "@t3tools/contracts/jones/installPlan";
import type { JonesRecoveryPreflight } from "@t3tools/contracts/jones/jonesOperations";
import { startPlan } from "./model.ts";
import { installPlanPresentation, recoveryPreflightPresentation } from "./presentation.ts";

const now = "2026-10-27T05:00:00Z",
  source = "a".repeat(40),
  tree = "b".repeat(40);
const env = EnvironmentId.make("laptop"),
  planId = "11111111-1111-4111-8111-111111111111";
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
