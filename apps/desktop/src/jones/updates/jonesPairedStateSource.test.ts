// @effect-diagnostics-next-line nodeBuiltinImport:off -- This byte-preservation test hashes the exact standalone Python payload.
import * as NodeCrypto from "node:crypto";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- The raw-source consumer must read the same tracked literals as the TypeScript composer.
import * as NodeFSP from "node:fs/promises";
import { describe, expect, it } from "vite-plus/test";
// @ts-expect-error -- This standalone Node fixture has no declaration file; its output is checked against the typed source exports below.
import * as ColdMaintenanceFixture from "../../../../../scripts/jones/updates/legacy-cold-maintenance-fixture.mjs";
import { jonesNativeHelperSource } from "./jonesNativeHelperSource.ts";
import { jonesPairedStateSource } from "./jonesPairedStateSource.ts";

const sourceTexts = () =>
  Promise.all([
    NodeFSP.readFile(new URL("./jonesNativeHelperSource.ts", import.meta.url), "utf8"),
    NodeFSP.readFile(new URL("./jonesPairedStateSource.ts", import.meta.url), "utf8"),
  ]);

describe("shared paired-state Python source", () => {
  it("preserves the executor bytes from the extraction baseline", () => {
    // Revision 3e466305 fixes the before/after behavior of this extraction;
    // intentional future executor changes must review this byte contract.
    expect(Buffer.byteLength(jonesNativeHelperSource)).toBe(70826);
    expect(NodeCrypto.createHash("sha256").update(jonesNativeHelperSource).digest("hex")).toBe(
      "eb98e75021ef01e59701039c3d402da202df2882c64c44f6fc44862725cbd934",
    );
    for (const fragment of Object.values(jonesPairedStateSource)) {
      expect(jonesNativeHelperSource.split(fragment)).toHaveLength(2);
    }
  });

  it("materializes the same executor for native qualification and TypeScript packaging", async () => {
    const [executorText, pairedText] = await sourceTexts();
    expect(ColdMaintenanceFixture.materializePairedExecutor(executorText, pairedText)).toBe(
      jonesNativeHelperSource,
    );
    expect((await ColdMaintenanceFixture.readColdMaintenanceSources()).executor).toBe(
      jonesNativeHelperSource,
    );
  });

  it("refuses missing, duplicate, unknown, and nonliteral substitutions", async () => {
    const [executorText, pairedText] = await sourceTexts();
    const reference = "${jonesPairedStateSource.syncTree}";
    for (const changed of [
      executorText.replace(reference, ""),
      executorText.replace(reference, () => reference + reference),
      executorText.replace(reference, "${jonesPairedStateSource.unknown}"),
      executorText.replace(reference, () => reference + "${unbound}"),
    ])
      expect(() => ColdMaintenanceFixture.materializePairedExecutor(changed, pairedText)).toThrow();
    for (const changed of [
      pairedText.replace("syncTree: String.raw`", "syncTree: String.raw`${unbound}"),
      pairedText.replace("copyAndTopology: String.raw`", "unknown: String.raw`"),
      pairedText.replace("} as const);", "extra: String.raw`unbound`,\n} as const);"),
      pairedText.replace("} as const);", "} as const);\nunbound();"),
    ])
      expect(() =>
        ColdMaintenanceFixture.materializePairedExecutor(executorText, changed),
      ).toThrow();
  });
});
