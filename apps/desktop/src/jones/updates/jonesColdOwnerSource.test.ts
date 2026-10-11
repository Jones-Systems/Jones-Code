// @effect-diagnostics-next-line nodeBuiltinImport:off -- Compare the literal source loader with packaged composition without starting an application runtime.
import * as NodeFSP from "node:fs/promises";
import { describe, expect, it } from "vite-plus/test";
// @ts-expect-error -- The standalone owned fixture has no declarations; source and bounded reports are checked below.
import * as ColdFixture from "../../../../../scripts/jones/updates/legacy-cold-maintenance-fixture.mjs";
import { jonesColdOwnerSource } from "./jonesColdOwnerSource.ts";
import { jonesLegacyMaintenanceSource } from "./jonesLegacyMaintenanceSource.ts";

const sourceTexts = () =>
  Promise.all(
    [
      "jonesLegacyBootstrapSource.ts",
      "jonesLegacyMaintenanceSource.ts",
      "jonesColdPairSource.ts",
      "jonesPairedStateSource.ts",
      "jonesColdOwnerSource.ts",
    ].map((name) => NodeFSP.readFile(new URL(name, import.meta.url), "utf8")),
  );

describe("continuous cold-owner source", () => {
  it("composes the internal issuer without adding a public state command", async () => {
    expect(jonesLegacyMaintenanceSource).toContain(jonesColdOwnerSource);
    expect(ColdFixture.materializeColdMaintenance(...(await sourceTexts()))).toBe(
      jonesLegacyMaintenanceSource,
    );
    expect(jonesLegacyMaintenanceSource).toContain(
      "for operation in ('arm', 'reconcile', 'cancel'):",
    );
    expect(jonesLegacyMaintenanceSource).toContain("del _cold_owner_bindings");
  });

  it("refuses missing, duplicate and nonliteral owner composition", async () => {
    const [bootstrap, maintenance, pair, paired, owner] = await sourceTexts();
    expect(maintenance).toBeDefined();
    expect(owner).toBeDefined();
    for (const changed of [
      maintenance!.replace("  jonesColdOwnerSource +", ""),
      maintenance!.replace(
        "  jonesColdOwnerSource +",
        "  jonesColdOwnerSource +\n  jonesColdOwnerSource +",
      ),
      maintenance!.replace("  jonesColdOwnerSource +", "  unboundOwner +"),
    ]) {
      expect(() =>
        ColdFixture.materializeColdMaintenance(bootstrap, changed, pair, paired, owner),
      ).toThrow();
    }
    for (const changed of [
      owner! + "\nunbound();",
      owner!.replace("String.raw`", "String.raw`${unbound}"),
      owner! + owner!,
      "unbound();\n" + owner!,
    ]) {
      expect(() =>
        ColdFixture.materializeColdMaintenance(bootstrap, maintenance, pair, paired, changed),
      ).toThrow();
    }
  });

  it.each(ColdFixture.coldOwnerCases as string[])(
    "holds the owned mechanical session boundary for %s",
    async (scenario) => {
      expect(await ColdFixture.runColdOwnerFixture(scenario)).toEqual({
        case: scenario,
        passed: true,
        qualification: "owned-mechanical-cold-owner",
        stateAdmissionAllowed: false,
        nativeColdOwnerQualified: false,
      });
    },
    30000,
  );
});
