// @effect-diagnostics nodeBuiltinImport:off - Pure packaging integrity checks.
import * as NodeCrypto from "node:crypto";
import { describe, expect, it } from "vite-plus/test";
import { jonesLegacyBootstrapSource } from "../../../apps/desktop/src/jones/updates/jonesLegacyBootstrapSource.ts";
import { jonesNativeHelperSource } from "../../../apps/desktop/src/jones/updates/jonesNativeHelperSource.ts";
import {
  createJonesLegacyBootstrapBundle,
  verifyJonesLegacyIdentityMachO,
} from "./legacy-bootstrap-bundle.ts";

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

function inertArm64Executable() {
  const bytes = Buffer.alloc(160);
  [0xfeedfacf, 0x0100000c, 0, 2, 2, 96, 0, 0].forEach((value, index) =>
    bytes.writeUInt32LE(value, index * 4),
  );
  bytes.writeUInt32LE(0x19, 32);
  bytes.writeUInt32LE(72, 36);
  bytes.write("__TEXT", 40, "ascii");
  bytes.writeBigUInt64LE(160n, 80);
  bytes.writeUInt32LE(0x80000028, 104);
  bytes.writeUInt32LE(24, 108);
  bytes.writeBigUInt64LE(128n, 112);
  return bytes;
}

describe("schema 2 maintenance resources", () => {
  it("binds binary bytes independently from the source and retains the v1 default", () => {
    const binary = inertArm64Executable();
    const files = createJonesLegacyBootstrapBundle(identity, {
      identityNative: binary,
      identityNativeSourceSha256: "c".repeat(64),
      maintenanceSource: "# inert maintenance fixture\n",
    });
    expect(files.map((file) => file.relativePath)).toEqual([
      "jones-bootstrap/bootstrap.py",
      "jones-bootstrap/executor.py",
      "jones-bootstrap/BUNDLE.json",
      "jones-bootstrap/maintenance.py",
      "jones-bootstrap/identity-native",
    ]);
    expect(files.map((file) => file.mode)).toEqual([0o500, 0o500, 0o400, 0o500, 0o500]);
    const manifest = JSON.parse(String(files[2]!.content));
    expect(Object.keys(manifest).sort()).toEqual(
      [
        "schema",
        "repository",
        "sourceSha",
        "sourceTree",
        "version",
        "bootstrapSha256",
        "executorSha256",
        "maintenanceSha256",
        "identityNativeSha256",
        "identityNativeSourceSha256",
        "nativeArchitecture",
      ].sort(),
    );
    expect(manifest).toMatchObject({
      schema: 2,
      nativeArchitecture: "arm64",
      identityNativeSourceSha256: "c".repeat(64),
    });
    expect(manifest.identityNativeSha256).toBe(
      NodeCrypto.createHash("sha256").update(binary).digest("hex"),
    );
    expect(manifest.maintenanceSha256).toBe(
      NodeCrypto.createHash("sha256").update(files[3]!.content).digest("hex"),
    );
    binary[159] = 255;
    expect(files[4]!.content).not.toEqual(binary);
    expect(JSON.parse(createJonesLegacyBootstrapBundle(identity)[2]!.content).schema).toBe(1);
  });

  it.each([
    [0, 0xcafebabe],
    [4, 0x01000007],
    [8, 2],
    [12, 6],
    [16, 0],
    [20, 4096],
    [28, 1],
    [36, 7],
    [108, 16],
  ])(
    "rejects wrong architecture, ABI, or malformed command tables at offset %i",
    (offset, value) => {
      const binary = inertArm64Executable();
      binary.writeUInt32LE(value, offset);
      expect(() => verifyJonesLegacyIdentityMachO(binary)).toThrow("thin arm64 Mach-O executable");
    },
  );
});

for (const terminator of ["\n", "\r", "\u2028", "\u2029"]) {
  it(`rejects trailing hash terminator ${JSON.stringify(terminator)} in bundle and proof identities`, () => {
    const native = {
      identityNative: inertArm64Executable(),
      identityNativeSourceSha256: "c".repeat(64),
      maintenanceSource: "# fixture\n",
    };
    for (const key of ["sourceSha", "sourceTree"] as const) {
      const changed = { ...identity, [key]: identity[key] + terminator };
      expect(() => createJonesLegacyBootstrapBundle(changed)).toThrow();
      expect(() => createJonesLegacyBootstrapBundle(changed, native)).toThrow();
    }
    expect(() =>
      createJonesLegacyBootstrapBundle(identity, {
        ...native,
        identityNativeSourceSha256: native.identityNativeSourceSha256 + terminator,
      }),
    ).toThrow();
    const proof = JSON.parse(
      String(createJonesLegacyBootstrapBundle(identity, native)[2]!.content),
    );
    expect(readJonesLegacyBootstrapNativeProof(JSON.stringify(proof), identity)).toEqual(proof);
    for (const key of [
      "sourceSha",
      "sourceTree",
      "bootstrapSha256",
      "executorSha256",
      "maintenanceSha256",
      "identityNativeSha256",
      "identityNativeSourceSha256",
    ]) {
      expect(() =>
        readJonesLegacyBootstrapNativeProof(
          JSON.stringify({ ...proof, [key]: proof[key] + terminator }),
          identity,
        ),
      ).toThrow();
    }
    const invalidIdentity = { ...identity, sourceSha: identity.sourceSha + terminator };
    expect(() =>
      readJonesLegacyBootstrapNativeProof(
        JSON.stringify({ ...proof, sourceSha: invalidIdentity.sourceSha }),
        invalidIdentity,
      ),
    ).toThrow();
  });
}

it("rejects proof extensions, architecture changes, and mismatched independent hashes", () => {
  const proof = JSON.parse(
    String(
      createJonesLegacyBootstrapBundle(identity, {
        identityNative: inertArm64Executable(),
        identityNativeSourceSha256: "c".repeat(64),
        maintenanceSource: "# fixture\n",
      })[2]!.content,
    ),
  );
  for (const changed of [
    { ...proof, path: "/untrusted" },
    { ...proof, nativeArchitecture: "x64" },
    { ...proof, schema: 1 },
    { ...proof, identityNativeSha256: "bad" },
  ])
    expect(() => readJonesLegacyBootstrapNativeProof(JSON.stringify(changed), identity)).toThrow();
});
