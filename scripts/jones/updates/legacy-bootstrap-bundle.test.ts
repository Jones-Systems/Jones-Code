// @effect-diagnostics nodeBuiltinImport:off - Pure packaging integrity checks.
import * as NodeCrypto from "node:crypto";
import { describe, expect, it } from "vite-plus/test";
import { jonesLegacyBootstrapSource } from "../../../apps/desktop/src/jones/updates/jonesLegacyBootstrapSource.ts";
import { jonesNativeHelperSource } from "../../../apps/desktop/src/jones/updates/jonesNativeHelperSource.ts";
import { createJonesLegacyBootstrapBundle } from "./legacy-bootstrap-bundle.ts";

const identity = {
  repository: "Jones-Systems/Jones-Code" as const,
  sourceSha: "a".repeat(40),
  sourceTree: "b".repeat(40),
  version: "0.0.45-preview.20261011.123.1",
};

describe("standalone legacy bootstrap packaging", () => {
  it("ships the exact standalone sources with independently verifiable provenance", () => {
    const files = createJonesLegacyBootstrapBundle(identity);
    expect(files.map((file) => file.relativePath)).toEqual([
      "jones-bootstrap/bootstrap.py",
      "jones-bootstrap/executor.py",
      "jones-bootstrap/BUNDLE.json",
    ]);
    expect(files[0]?.content).toBe(jonesLegacyBootstrapSource);
    expect(files[1]?.content).toBe(jonesNativeHelperSource);
    expect(files.map((file) => file.mode)).toEqual([0o500, 0o500, 0o400]);
    const manifest = JSON.parse(files[2]!.content);
    expect(manifest).toMatchObject({ schema: 1, ...identity });
    expect(manifest.bootstrapSha256).toBe(
      NodeCrypto.createHash("sha256").update(files[0]!.content).digest("hex"),
    );
    expect(manifest.executorSha256).toBe(
      NodeCrypto.createHash("sha256").update(files[1]!.content).digest("hex"),
    );
    expect(manifest).not.toHaveProperty("appDigest");
    expect(createJonesLegacyBootstrapBundle(identity)).toEqual(files);
  });

  it.each([
    { sourceSha: "a".repeat(39) },
    { sourceTree: "../unbound" },
    { version: "0.0.45" },
    { version: "0.0.45-preview.20261011.123.1\n" },
  ])("refuses an unqualified packaging identity %j", (changed) => {
    expect(() => createJonesLegacyBootstrapBundle({ ...identity, ...changed })).toThrow(
      "exact Jones preview provenance",
    );
  });
});
