// @effect-diagnostics nodeBuiltinImport:off - Pure packaging of the standalone native updater tools.
import * as NodeCrypto from "node:crypto";
import { jonesLegacyBootstrapSource } from "../../../apps/desktop/src/jones/updates/jonesLegacyBootstrapSource.ts";
import { jonesNativeHelperSource } from "../../../apps/desktop/src/jones/updates/jonesNativeHelperSource.ts";

export interface JonesLegacyBootstrapBundleInput {
  readonly repository: "Jones-Systems/Jones-Code";
  readonly sourceSha: string;
  readonly sourceTree: string;
  readonly version: string;
}

export interface JonesLegacyBootstrapBundleFile {
  readonly relativePath: string;
  readonly content: string;
  readonly mode: number;
}

export function createJonesLegacyBootstrapBundle(
  input: JonesLegacyBootstrapBundleInput,
): ReadonlyArray<JonesLegacyBootstrapBundleFile> {
  if (
    input.repository !== "Jones-Systems/Jones-Code" ||
    input.sourceSha.length !== 40 ||
    !/^[a-f0-9]{40}$/.test(input.sourceSha) ||
    input.sourceTree.length !== 40 ||
    !/^[a-f0-9]{40}$/.test(input.sourceTree) ||
    !/^\d+\.\d+\.\d+-preview\.\d{8}\.\d+(?:\.\d+)?$/.test(input.version) ||
    input.version.length > 128 ||
    /\s/.test(input.version)
  )
    throw new Error("Legacy bootstrap packaging requires exact Jones preview provenance.");
  const sha256 = (value: string) => NodeCrypto.createHash("sha256").update(value).digest("hex");
  return [
    {
      relativePath: "jones-bootstrap/bootstrap.py",
      content: jonesLegacyBootstrapSource,
      mode: 0o500,
    },
    { relativePath: "jones-bootstrap/executor.py", content: jonesNativeHelperSource, mode: 0o500 },
    {
      relativePath: "jones-bootstrap/BUNDLE.json",
      content:
        JSON.stringify({
          schema: 1,
          repository: input.repository,
          sourceSha: input.sourceSha,
          sourceTree: input.sourceTree,
          version: input.version,
          bootstrapSha256: sha256(jonesLegacyBootstrapSource),
          executorSha256: sha256(jonesNativeHelperSource),
        }) + "\n",
      mode: 0o400,
    },
  ];
}
