// @effect-diagnostics-next-line nodeBuiltinImport:off -- Compare the raw fixture materializer with the packaged TypeScript composition.
import * as NodeFSP from "node:fs/promises";
import { describe, expect, it } from "vite-plus/test";
// @ts-expect-error -- The standalone Node qualifier has no declarations; its returned source and reports are checked here.
import * as ColdFixture from "../../../../../scripts/jones/updates/legacy-cold-maintenance-fixture.mjs";
import { jonesColdPairSource } from "./jonesColdPairSource.ts";
import { jonesLegacyMaintenanceSource } from "./jonesLegacyMaintenanceSource.ts";

const sourceTexts = () =>
  Promise.all(
    [
      "jonesLegacyBootstrapSource.ts",
      "jonesLegacyMaintenanceSource.ts",
      "jonesColdPairSource.ts",
      "jonesPairedStateSource.ts",
    ].map((name) => NodeFSP.readFile(new URL(name, import.meta.url), "utf8")),
  );

describe("cold paired transaction source", () => {
  it("materializes the same private definitions for packaging and native qualification", async () => {
    expect(jonesLegacyMaintenanceSource).toContain(jonesColdPairSource);
    expect(ColdFixture.materializeColdMaintenance(...(await sourceTexts()))).toBe(
      jonesLegacyMaintenanceSource,
    );
    expect((await ColdFixture.readColdMaintenanceSources()).maintenance).toBe(
      jonesLegacyMaintenanceSource,
    );
    expect(jonesLegacyMaintenanceSource).not.toContain("$" + "{");
  });

  it("refuses missing, duplicate, unknown and untransformed primitive substitutions", async () => {
    const [bootstrap, maintenance, cold, paired] = await sourceTexts();
    expect(cold).toBeDefined();
    const source = cold!;
    const reference = "${indentColdStorage(jonesPairedStateSource.syncTree)}";
    for (const changed of [
      source.replace(reference, ""),
      source.replace(reference, () => reference + reference),
      source.replace(reference, "${indentColdStorage(jonesPairedStateSource.unknown)}"),
      source.replace(reference, "${jonesPairedStateSource.syncTree}"),
      source.replace(reference, () => reference + "${unbound}"),
      source + "\nunbound();",
    ]) {
      expect(() =>
        ColdFixture.materializeColdMaintenance(bootstrap, maintenance, changed, paired),
      ).toThrow();
    }
    expect(() =>
      ColdFixture.materializeColdMaintenance(
        bootstrap,
        maintenance!.replace("  jonesColdPairSource +", ""),
        source,
        paired,
      ),
    ).toThrow();
  });

  it.each(ColdFixture.coldPairCases as string[])(
    "retains the cold-pair contract for %s",
    async (scenario) => {
      const result = await ColdFixture.runColdPairFixture(scenario);
      expect(result).toEqual({
        case: scenario,
        passed: true,
        qualification: "owned-mechanical-storage",
        stateAdmissionAllowed: false,
      });
    },
    30000,
  );
});
