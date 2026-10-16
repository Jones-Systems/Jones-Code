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
      "jonesPreparationWorkerSource.ts",
      "jonesColdReceiverSource.ts",
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

  it("refuses missing, duplicate and nonliteral worker composition", async () => {
    const [bootstrap, maintenance, pair, paired, owner, worker] = await sourceTexts();
    for (const changed of [
      maintenance!.replace("  jonesPreparationWorkerSource +", ""),
      maintenance!.replace(
        "  jonesPreparationWorkerSource +",
        "  jonesPreparationWorkerSource +\n  jonesPreparationWorkerSource +",
      ),
      maintenance!.replace("  jonesPreparationWorkerSource +", "  unboundWorker +"),
    ]) {
      expect(() =>
        ColdFixture.materializeColdMaintenance(bootstrap, changed, pair, paired, owner, worker),
      ).toThrow();
    }
    for (const changed of [
      worker! + "\nunbound();",
      worker!.replace("String.raw`", "String.raw`${unbound}"),
      worker! + worker!,
      "unbound();\n" + worker!,
    ]) {
      expect(() =>
        ColdFixture.materializeColdMaintenance(
          bootstrap,
          maintenance,
          pair,
          paired,
          owner,
          changed,
        ),
      ).toThrow();
    }
  });

  it("refuses missing, duplicate and nonliteral receiver composition", async () => {
    const [bootstrap, maintenance, pair, paired, owner, worker, receiver] = await sourceTexts();
    for (const changed of [
      maintenance!.replace("  jonesColdReceiverSource +", ""),
      maintenance!.replace(
        "  jonesColdReceiverSource +",
        "  jonesColdReceiverSource +\n  jonesColdReceiverSource +",
      ),
      maintenance!.replace("  jonesColdReceiverSource +", "  unboundReceiver +"),
    ]) {
      expect(() =>
        ColdFixture.materializeColdMaintenance(
          bootstrap,
          changed,
          pair,
          paired,
          owner,
          worker,
          receiver,
        ),
      ).toThrow();
    }
    for (const changed of [
      receiver! + "\nunbound();",
      receiver! + receiver!,
      receiver!.replace("String.raw`", "String.raw`${unbound}"),
      "unbound();\n" + receiver!,
    ]) {
      expect(() =>
        ColdFixture.materializeColdMaintenance(
          bootstrap,
          maintenance,
          pair,
          paired,
          owner,
          worker,
          changed,
        ),
      ).toThrow();
    }
  });

  it.each(ColdFixture.coldPreparationCases as string[])(
    "holds the captured preparation boundary for %s",
    async (scenario) => {
      expect(await ColdFixture.runColdPreparationFixture(scenario)).toEqual({
        case: scenario,
        passed: true,
        qualification: "owned-mechanical-captured-preparation",
        stateAdmissionAllowed: false,
        nativeColdOwnerQualified: false,
        sqliteQualified: false,
        bootQualification: "synthetic-prior-boot",
      });
    },
    30000,
  );

  it.each(ColdFixture.coldRecoveryCases as string[])(
    "recovers the original app after restored state only within live custody for %s",
    async (scenario) => {
      expect(await ColdFixture.runColdRecoveryFixture(scenario)).toEqual({
        case: scenario,
        passed: true,
        qualification: "owned-mechanical-cold-recovery",
        stateAdmissionAllowed: false,
        nativeColdOwnerQualified: false,
        sqliteQualified: false,
        bootQualification: "synthetic-prior-boot",
      });
    },
    30000,
  );

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
