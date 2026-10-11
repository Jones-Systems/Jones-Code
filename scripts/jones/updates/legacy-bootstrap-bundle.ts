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

export interface JonesLegacyBootstrapBundleFile<Content extends string | Uint8Array = string> {
  readonly relativePath: string;
  readonly content: Content;
  readonly mode: number;
}

export interface JonesLegacyBootstrapNativeInput {
  readonly identityNative: Uint8Array;
  readonly identityNativeSourceSha256: string;
  readonly maintenanceSource: string;
}

type TextBundleFile = Omit<JonesLegacyBootstrapBundleFile, "content"> & {
  readonly content: string;
};

/** Accept only a bounded, thin ARM64 executable with a complete load-command table. */
export function verifyJonesLegacyIdentityMachO(bytes: Uint8Array): void {
  const data = Buffer.from(bytes);
  const invalid = () => {
    throw new Error("Legacy identity helper must be a thin arm64 Mach-O executable.");
  };
  if (data.length < 32 || data.length > 4 * 1024 * 1024) invalid();
  if (
    data.readUInt32LE(0) !== 0xfeedfacf ||
    data.readUInt32LE(4) !== 0x0100000c ||
    data.readUInt32LE(8) !== 0 ||
    data.readUInt32LE(12) !== 2 ||
    data.readUInt32LE(28) !== 0
  )
    invalid();
  const count = data.readUInt32LE(16);
  const end = 32 + data.readUInt32LE(20);
  if (count < 1 || count > 512 || end > data.length) invalid();
  let offset = 32;
  let entry = false;
  let text = false;
  for (let index = 0; index < count; index++) {
    if (offset + 8 > end) invalid();
    const command = data.readUInt32LE(offset);
    const size = data.readUInt32LE(offset + 4);
    if (size < 8 || size % 8 !== 0 || offset + size > end) invalid();
    if (command === 0x80000028) {
      if (size !== 24 || data.readBigUInt64LE(offset + 8) >= BigInt(data.length)) invalid();
      entry = true;
    }
    if (command === 0x19) {
      if (size < 72 || size !== 72 + data.readUInt32LE(offset + 64) * 80) invalid();
      const fileOffset = data.readBigUInt64LE(offset + 40);
      const fileSize = data.readBigUInt64LE(offset + 48);
      if (fileOffset + fileSize > BigInt(data.length)) invalid();
      if (
        data
          .subarray(offset + 8, offset + 24)
          .toString("ascii")
          .replace(/\0+$/, "") === "__TEXT"
      )
        text = fileOffset === 0n && fileSize > 0n;
    }
    offset += size;
  }
  if (offset !== end || !entry || !text) invalid();
}

export function createJonesLegacyBootstrapBundle(
  input: JonesLegacyBootstrapBundleInput,
): ReadonlyArray<TextBundleFile>;
export function createJonesLegacyBootstrapBundle(
  input: JonesLegacyBootstrapBundleInput,
  native: JonesLegacyBootstrapNativeInput,
): ReadonlyArray<JonesLegacyBootstrapBundleFile<string | Uint8Array>>;
export function createJonesLegacyBootstrapBundle(
  input: JonesLegacyBootstrapBundleInput,
  native?: JonesLegacyBootstrapNativeInput,
): ReadonlyArray<JonesLegacyBootstrapBundleFile<string | Uint8Array>> {
  if (
    input.repository !== "Jones-Systems/Jones-Code" ||
    !isHash(input.sourceSha, 40) ||
    !isHash(input.sourceTree, 40) ||
    !/^\d+\.\d+\.\d+-preview\.\d{8}\.\d+(?:\.\d+)?$/.test(input.version) ||
    input.version.length > 128 ||
    /\s/.test(input.version)
  )
    throw new Error("Legacy bootstrap packaging requires exact Jones preview provenance.");
  const sha256 = (value: string | Uint8Array) =>
    NodeCrypto.createHash("sha256").update(value).digest("hex");
  if (native !== undefined) {
    verifyJonesLegacyIdentityMachO(native.identityNative);
    if (
      !isHash(native.identityNativeSourceSha256, 64) ||
      native.maintenanceSource.length === 0 ||
      Buffer.byteLength(native.maintenanceSource) > 1048576
    )
      throw new Error("Legacy maintenance packaging requires exact source bytes.");
  }
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
          schema: native === undefined ? 1 : 2,
          repository: input.repository,
          sourceSha: input.sourceSha,
          sourceTree: input.sourceTree,
          version: input.version,
          bootstrapSha256: sha256(jonesLegacyBootstrapSource),
          executorSha256: sha256(jonesNativeHelperSource),
          ...(native === undefined
            ? {}
            : {
                maintenanceSha256: sha256(native.maintenanceSource),
                identityNativeSha256: sha256(native.identityNative),
                identityNativeSourceSha256: native.identityNativeSourceSha256,
                nativeArchitecture: "arm64",
              }),
        }) + "\n",
      mode: 0o400,
    },
    ...(native === undefined
      ? []
      : [
          {
            relativePath: "jones-bootstrap/maintenance.py",
            content: native.maintenanceSource,
            mode: 0o500,
          },
          {
            relativePath: "jones-bootstrap/identity-native",
            content: Uint8Array.from(native.identityNative),
            mode: 0o500,
          },
        ]),
  ];
}

export interface JonesLegacyBootstrapNativeProof extends JonesLegacyBootstrapBundleInput {
  readonly schema: 2;
  readonly bootstrapSha256: string;
  readonly executorSha256: string;
  readonly maintenanceSha256: string;
  readonly identityNativeSha256: string;
  readonly identityNativeSourceSha256: string;
  readonly nativeArchitecture: "arm64";
}

/** This record is supplied independently by the build, never read from the copied bundle. */
export function readJonesLegacyBootstrapNativeProof(
  raw: string,
  identity: JonesLegacyBootstrapBundleInput,
): JonesLegacyBootstrapNativeProof {
  if (Buffer.byteLength(raw) > 8192) throw new Error("Native build proof exceeds its bound.");
  const value: unknown = JSON.parse(raw);
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new Error("Native build proof is not an object.");
  const proof = value as Record<string, unknown>;
  const hashes = [
    "bootstrapSha256",
    "executorSha256",
    "maintenanceSha256",
    "identityNativeSha256",
    "identityNativeSourceSha256",
  ];
  const keys = [
    "schema",
    "repository",
    "sourceSha",
    "sourceTree",
    "version",
    "nativeArchitecture",
    ...hashes,
  ];
  if (
    !isHash(identity.sourceSha, 40) ||
    !isHash(identity.sourceTree, 40) ||
    Object.keys(proof).sort().join(",") !== keys.sort().join(",") ||
    proof.schema !== 2 ||
    proof.nativeArchitecture !== "arm64" ||
    Object.entries(identity).some(([key, expected]) => proof[key] !== expected) ||
    hashes.some((key) => !isHash(proof[key], 64))
  )
    throw new Error("Native build proof does not match the exact source and architecture.");
  return proof as unknown as JonesLegacyBootstrapNativeProof;
}
