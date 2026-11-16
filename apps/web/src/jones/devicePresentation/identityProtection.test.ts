import { expect, it } from "vite-plus/test";
import { deviceDisplayLabel } from "@t3tools/contracts/jones/devicePresentation";
import { splitSharedServerPatch } from "@t3tools/client-runtime/state/shared-settings";
import { resolveShellDestination } from "../shellDestination/model";
import {
  excludedSidebarEnvironmentIds,
  DEFAULT_SIDEBAR_ENVIRONMENT_FILTERS,
} from "../environmentFilters/model";

it("keeps each device name on its owning server instead of synchronizing it to other machines", () => {
  expect(
    splitSharedServerPatch({ deviceDisplayName: "Office", sidebarAutoSettleOnMerge: true }),
  ).toEqual({
    localPatch: { deviceDisplayName: "Office" },
    sharedPatch: { sidebarAutoSettleOnMerge: true },
  });
});

it("keeps duplicate cosmetic aliases out of shell label assertions and legacy environment filters", () => {
  const environments = [
    {
      environmentId: "mini-id",
      label: "Mac Mini",
      serverLabel: "Original Mac",
      phase: "connected",
      cwd: "/synthetic",
      canOperate: true,
    },
    {
      environmentId: "vps-id",
      label: "VPS",
      phase: "connected",
      cwd: "/synthetic",
      canOperate: true,
    },
  ].map((environment) => ({
    ...environment,
    displayLabel: deviceDisplayLabel(environment.label, null, "Office"),
  }));
  expect(environments.map((environment) => environment.displayLabel)).toEqual(["Office", "Office"]);
  expect(
    resolveShellDestination(
      { _tag: "Valid", environmentId: "mini-id", label: "Office" },
      "source",
      environments,
    ).status,
  ).toBe("label-mismatch");
  expect(
    resolveShellDestination(
      { _tag: "Valid", environmentId: "mini-id", label: "Original Mac" },
      "source",
      environments,
    ).status,
  ).toBe("ready");
  expect([
    ...excludedSidebarEnvironmentIds(environments, {
      ...DEFAULT_SIDEBAR_ENVIRONMENT_FILTERS,
      Mini: false,
    }),
  ]).toEqual(["mini-id"]);
});
