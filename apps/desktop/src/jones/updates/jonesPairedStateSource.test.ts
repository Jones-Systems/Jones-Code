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
  it("pins the paired-restore authority transition executor", () => {
    // Intentional executor changes update this byte contract together with the
    // source-qualified producer pins and transaction safety tests.
    expect(Buffer.byteLength(jonesNativeHelperSource)).toBe(84524);
    expect(NodeCrypto.createHash("sha256").update(jonesNativeHelperSource).digest("hex")).toBe(
      "fa740f485c6c6cc78835edf8fa163b439a85927e0b18ff1897473b6b82b2f5d1",
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
