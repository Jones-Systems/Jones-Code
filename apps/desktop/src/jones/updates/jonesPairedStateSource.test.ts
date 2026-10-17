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
  it("matches the reviewed manual-install executor baseline", () => {
    // Revision b068e7b1 adds the reviewed controlled manual-install entry;
    // intentional future executor changes must review this byte contract.
    expect(Buffer.byteLength(jonesNativeHelperSource)).toBe(79291);
    expect(NodeCrypto.createHash("sha256").update(jonesNativeHelperSource).digest("hex")).toBe(
      "6a354259472cd591ccedabf56ef503547d729ef7c6747a0b99041137160652e4",
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
