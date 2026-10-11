// @effect-diagnostics nodeBuiltinImport:off - Pure packaging integrity checks.
import * as NodeCrypto from "node:crypto";
import { describe, expect, it } from "vite-plus/test";
import { jonesLegacyBootstrapSource } from "../../../apps/desktop/src/jones/updates/jonesLegacyBootstrapSource.ts";
import { jonesNativeHelperSource } from "../../../apps/desktop/src/jones/updates/jonesNativeHelperSource.ts";
import {
  createJonesLegacyBootstrapBundle,
  createJonesLegacyBootstrapPreparationBundle,
  JONES_PREPARATION_BOUNDS,
  JONES_PREPARATION_SOURCE_INPUTS,
  readJonesPreparationArtifactEvidence,
  readJonesLegacyBootstrapPreparationProof,
  type JonesPreparationArtifactEvidence,
  readJonesLegacyBootstrapNativeProof,
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

it.each(["\n", "\r", "\u2028", "\u2029"])(
  "rejects trailing hash terminator %j in bundle and proof identities",
  (terminator) => {
    const native = {
      identityNative: inertArm64Executable(),
      identityNativeSourceSha256: "c".repeat(64),
      maintenanceSource: "# fixture\n",
    };
    for (const key of ["sourceSha", "sourceTree"] as const) {
      const changed = { ...identity, [key]: identity[key] + terminator };
      expect(() => createJonesLegacyBootstrapBundle(changed)).toThrow();
      expect(() => createJonesLegacyBootstrapBundle(changed, native)).toThrow();
      expect(() =>
        createJonesLegacyBootstrapBundle({
          ...identity,
          [key]: identity[key].slice(1) + terminator,
        }),
      ).toThrow();
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
  },
);

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

function preparationFixture(): JonesPreparationArtifactEvidence {
  // These are synthetic proof data, never an observed or executable preparation artifact.
  const seaConfig =
    '{"main":"offlinePreparationEntry.mjs","mainFormat":"module","executable":"node","output":"jones-offline-preparation","disableExperimentalSEAWarning":true,"useSnapshot":false,"useCodeCache":false,"execArgv":[],"execArgvExtension":"none"}\n';
  return {
    schema: 1,
    observation: "complete",
    classification: "cold-preparation-worker",
    source: { ...identity },
    preparation: {
      schema: 1,
      protocol: 1,
      provenanceSha256: "1".repeat(64),
      sourceInputHashes: Object.fromEntries(
        JONES_PREPARATION_SOURCE_INPUTS.map((name) => [name, "2".repeat(64)]),
      ) as JonesPreparationArtifactEvidence["preparation"]["sourceInputHashes"],
      entry: { sha256: "3".repeat(64), size: 4096 },
      seaConfig: {
        sha256: NodeCrypto.createHash("sha256").update(seaConfig).digest("hex"),
        size: Buffer.byteLength(seaConfig),
      },
      executable: {
        name: "jones-offline-preparation",
        sha256: "4".repeat(64),
        size: 96 * 1024 * 1024,
      },
      node: {
        version: "26.8.2",
        platform: "darwin",
        architecture: "arm64",
        archiveSha256: "5".repeat(64),
        archiveBytes: 48 * 1024 * 1024,
        checksumsSha256: "6".repeat(64),
        runtimeSha256: "7".repeat(64),
        runtimeBytes: 80 * 1024 * 1024,
      },
      sqlite: { version: "3.50.4", sourceId: "2025-07-30 19:33:53 " + "8".repeat(64) },
    },
  };
}

function preparationNativeFixture() {
  return {
    identityNative: inertArm64Executable(),
    identityNativeSourceSha256: "c".repeat(64),
    maintenanceSource: "# inert maintenance fixture\n",
  };
}

const proofJson = (value: unknown) => JSON.stringify(value) + "\n";
const preparationWorkerInput = "apps/server/src/jones/updates/offlinePreparationWorker.ts";

describe("schema 3 preparation data contract", () => {
  it("describes one large streamed runtime while preserving every v2 resource and the v1 default", () => {
    const native = preparationNativeFixture();
    const evidence = preparationFixture();
    const previous = createJonesLegacyBootstrapBundle(identity, native);
    const bundle = createJonesLegacyBootstrapPreparationBundle(identity, native, evidence);
    expect(bundle.files).toHaveLength(5);
    expect(bundle.runtime).toEqual({
      kind: "streamed-file",
      relativePath: "jones-bootstrap/jones-offline-preparation",
      mode: 0o500,
      sha256: evidence.preparation.executable.sha256,
      size: 96 * 1024 * 1024,
      maxBytes: 256 * 1024 * 1024,
    });
    expect(bundle.runtime).not.toHaveProperty("content");
    expect(bundle.runtime).not.toHaveProperty("sourcePath");
    expect(bundle.files.filter((file) => !file.relativePath.endsWith("BUNDLE.json"))).toEqual(
      previous.filter((file) => !file.relativePath.endsWith("BUNDLE.json")),
    );
    const manifest = readJonesLegacyBootstrapPreparationProof(
      String(bundle.files[2]!.content),
      identity,
    );
    const { preparation, ...base } = manifest;
    expect(base).toEqual({ ...JSON.parse(String(previous[2]!.content)), schema: 3 });
    expect(preparation).toEqual(evidence.preparation);
    expect(manifest).not.toHaveProperty("activationAllowed");
    expect(createJonesLegacyBootstrapBundle(identity, native)).toEqual(previous);
    expect(JSON.parse(createJonesLegacyBootstrapBundle(identity)[2]!.content).schema).toBe(1);
    expect(() =>
      readJonesLegacyBootstrapNativeProof(String(bundle.files[2]!.content), identity),
    ).toThrow();
    expect(() =>
      readJonesLegacyBootstrapPreparationProof(String(previous[2]!.content), identity),
    ).toThrow();
  });

  it("decodes a bounded immutable snapshot without retaining the producer's mutable records", () => {
    const source = preparationFixture();
    const decoded = readJonesPreparationArtifactEvidence(proofJson(source), identity);
    expect(decoded).toEqual(source);
    const bundle = createJonesLegacyBootstrapPreparationBundle(
      identity,
      preparationNativeFixture(),
      source,
    );
    const manifest = String(bundle.files[2]!.content);
    const workerHash = decoded.preparation.sourceInputHashes[preparationWorkerInput];
    Object.assign(source.preparation.sourceInputHashes, {
      [preparationWorkerInput]: "f".repeat(64),
    });
    Object.assign(source.preparation.executable, { size: 1, sha256: "9".repeat(64) });
    Object.assign(source.source, { sourceSha: "f".repeat(40) });
    expect(decoded.preparation.executable.size).toBe(96 * 1024 * 1024);
    expect(decoded.preparation.sourceInputHashes[preparationWorkerInput]).toBe(workerHash);
    expect(bundle.runtime.size).toBe(decoded.preparation.executable.size);
    expect(String(bundle.files[2]!.content)).toBe(manifest);
    for (const value of [
      decoded,
      decoded.source,
      decoded.preparation,
      decoded.preparation.sourceInputHashes,
      decoded.preparation.entry,
      decoded.preparation.seaConfig,
      decoded.preparation.executable,
      decoded.preparation.node,
      decoded.preparation.sqlite,
    ])
      expect(Object.isFrozen(value)).toBe(true);
  });

  it.each([
    { schema: 2 },
    { observation: "incomplete" },
    { classification: "held-entry-only" },
    { preparationCapable: true },
    { runtimePath: "/caller/tool" },
  ])("rejects held, partial or extended producer evidence %j", (changed) => {
    const evidence = { ...preparationFixture(), ...changed };
    expect(() => readJonesPreparationArtifactEvidence(proofJson(evidence), identity)).toThrow();
  });

  it.each([
    { repository: "foreign/repository" },
    { sourceSha: "e".repeat(40) },
    { sourceTree: "f".repeat(40) },
    { version: "0.0.45-preview.20261011.124.1" },
    { sourceSha: "a".repeat(39) + "\n" },
    { version: identity.version + "\n" },
    { extra: true },
  ])("rejects artifact/source binding variance %j", (changed) => {
    const evidence = preparationFixture();
    expect(() =>
      readJonesPreparationArtifactEvidence(
        proofJson({ ...evidence, source: { ...evidence.source, ...changed } }),
        identity,
      ),
    ).toThrow();
  });

  it.each([
    ["schema", 2],
    ["protocol", 0],
    ["protocol", 2],
    ["protocol", true],
    ["provenanceSha256", "a".repeat(63)],
    ["provenanceSha256", "A".repeat(64)],
    ["provenanceSha256", "a".repeat(63) + "\n"],
    ["activationAllowed", true],
  ] as const)("rejects invalid preparation field %s=%j", (key, value) => {
    const evidence = preparationFixture();
    expect(() =>
      readJonesPreparationArtifactEvidence(
        proofJson({
          ...evidence,
          preparation: { ...evidence.preparation, [key]: value },
        }),
        identity,
      ),
    ).toThrow();
  });

  it.each(["entry", "seaConfig", "executable", "node", "sqlite", "sourceInputHashes"] as const)(
    "rejects unknown keys and missing nested records in %s",
    (key) => {
      const evidence = preparationFixture();
      expect(() =>
        readJonesPreparationArtifactEvidence(
          proofJson({
            ...evidence,
            preparation: {
              ...evidence.preparation,
              [key]: { ...evidence.preparation[key], path: "/caller" },
            },
          }),
          identity,
        ),
      ).toThrow();
      expect(() =>
        readJonesPreparationArtifactEvidence(
          proofJson({
            ...evidence,
            preparation: { ...evidence.preparation, [key]: null },
          }),
          identity,
        ),
      ).toThrow();
    },
  );

  it("requires exactly fifteen source inputs including the core, plan codec and worker", () => {
    const evidence = preparationFixture();
    expect(JONES_PREPARATION_SOURCE_INPUTS).toHaveLength(15);
    expect(JONES_PREPARATION_SOURCE_INPUTS).toContain(preparationWorkerInput);
    expect(JONES_PREPARATION_SOURCE_INPUTS).toContain(
      "apps/server/src/jones/updates/offlinePreparationCore.ts",
    );
    expect(JONES_PREPARATION_SOURCE_INPUTS).toContain(
      "apps/server/src/jones/updates/offlinePreparationPlan.ts",
    );
    for (const name of JONES_PREPARATION_SOURCE_INPUTS) {
      const missing: Record<string, string> = { ...evidence.preparation.sourceInputHashes };
      delete missing[name];
      for (const hashes of [
        missing,
        { ...evidence.preparation.sourceInputHashes, [name]: "unknown" },
      ]) {
        expect(() =>
          readJonesPreparationArtifactEvidence(
            proofJson({
              ...evidence,
              preparation: { ...evidence.preparation, sourceInputHashes: hashes },
            }),
            identity,
          ),
        ).toThrow();
      }
    }
  });

  it.each(["missing-worker", "invalid-worker-hash", "rebound-worker", "extra-worker"] as const)(
    "refuses %s in producer evidence, typed packaging and copied bundle proof",
    (scenario) => {
      const evidence = preparationFixture();
      const valid = createJonesLegacyBootstrapPreparationBundle(
        identity,
        preparationNativeFixture(),
        evidence,
      );
      const hashes: Record<string, string> = { ...evidence.preparation.sourceInputHashes };
      if (scenario === "missing-worker") {
        delete hashes[preparationWorkerInput];
        expect(Object.keys(hashes)).toHaveLength(14);
      }
      if (scenario === "invalid-worker-hash")
        hashes[preparationWorkerInput] = "2".repeat(63) + "\n";
      if (scenario === "rebound-worker") {
        delete hashes[preparationWorkerInput];
        hashes["apps/server/src/jones/updates/../updates/offlinePreparationWorker.ts"] = "2".repeat(
          64,
        );
      }
      if (scenario === "extra-worker")
        hashes["apps/server/src/jones/updates/offlinePreparationWorker.test.ts"] = "2".repeat(64);
      const changed = {
        ...evidence,
        preparation: { ...evidence.preparation, sourceInputHashes: hashes },
      };
      expect(() => readJonesPreparationArtifactEvidence(proofJson(changed), identity)).toThrow();
      Object.assign(evidence.preparation, { sourceInputHashes: hashes });
      expect(() =>
        createJonesLegacyBootstrapPreparationBundle(identity, preparationNativeFixture(), evidence),
      ).toThrow();
      const proof = JSON.parse(String(valid.files[2]!.content));
      proof.preparation.sourceInputHashes = hashes;
      expect(() => readJonesLegacyBootstrapPreparationProof(proofJson(proof), identity)).toThrow();
    },
  );

  it.each([
    ["version", "26.8.3"],
    ["platform", "linux"],
    ["architecture", "x64"],
    ["archiveSha256", "missing"],
    ["checksumsSha256", "missing"],
    ["runtimeSha256", "missing"],
    ["archiveBytes", 128 * 1024 * 1024 + 1],
    ["runtimeBytes", 256 * 1024 * 1024 + 1],
  ] as const)("rejects unknown or malformed pinned Node observation %s=%j", (key, value) => {
    const evidence = preparationFixture();
    expect(() =>
      readJonesPreparationArtifactEvidence(
        proofJson({
          ...evidence,
          preparation: {
            ...evidence.preparation,
            node: { ...evidence.preparation.node, [key]: value },
          },
        }),
        identity,
      ),
    ).toThrow();
  });

  it.each([
    { version: "unknown" },
    { version: "3.50.4\n" },
    { version: "03.50.4" },
    { sourceId: "unknown" },
    { sourceId: "2025-02-30 19:33:53 " + "8".repeat(64) },
    { sourceId: "2025-07-30 19:33:53 " + "A".repeat(64) },
    { sourceId: "2025-07-30 19:33:53 " + "8".repeat(64) + "\n" },
  ])("rejects missing or malformed SQLite observations %j", (changed) => {
    const evidence = preparationFixture();
    expect(() =>
      readJonesPreparationArtifactEvidence(
        proofJson({
          ...evidence,
          preparation: {
            ...evidence.preparation,
            sqlite: { ...evidence.preparation.sqlite, ...changed },
          },
        }),
        identity,
      ),
    ).toThrow();
  });

  it.each([0, -1, 0.5, Number.MAX_SAFE_INTEGER + 1, 256 * 1024 * 1024 + 1, "100"])(
    "refuses invalid executable byte count %j",
    (size) => {
      const evidence = preparationFixture();
      expect(() =>
        readJonesPreparationArtifactEvidence(
          proofJson({
            ...evidence,
            preparation: {
              ...evidence.preparation,
              executable: { ...evidence.preparation.executable, size },
            },
          }),
          identity,
        ),
      ).toThrow();
    },
  );

  it("accepts the exact runtime limit without allocating bytes and rejects an oversized entry", () => {
    const evidence = preparationFixture();
    const maximum = {
      ...evidence,
      preparation: {
        ...evidence.preparation,
        executable: {
          ...evidence.preparation.executable,
          size: JONES_PREPARATION_BOUNDS.runtimeBytes,
        },
      },
    };
    expect(
      createJonesLegacyBootstrapPreparationBundle(identity, preparationNativeFixture(), maximum)
        .runtime.size,
    ).toBe(256 * 1024 * 1024);
    expect(() =>
      readJonesPreparationArtifactEvidence(
        proofJson({
          ...evidence,
          preparation: {
            ...evidence.preparation,
            entry: {
              ...evidence.preparation.entry,
              size: 32 * 1024 * 1024 + 1,
            },
          },
        }),
        identity,
      ),
    ).toThrow();
  });

  it.each([{ sha256: "f".repeat(64) }, { size: 1 }])(
    "rejects a different SEA configuration even with a well-formed digest record %j",
    (changed) => {
      const evidence = preparationFixture();
      expect(() =>
        readJonesPreparationArtifactEvidence(
          proofJson({
            ...evidence,
            preparation: {
              ...evidence.preparation,
              seaConfig: { ...evidence.preparation.seaConfig, ...changed },
            },
          }),
          identity,
        ),
      ).toThrow();
    },
  );

  it("rejects duplicate keys, noncanonical JSON and over-bound proof bytes", () => {
    const evidence = preparationFixture();
    const raw = proofJson(evidence);
    for (const invalid of [
      raw.replace('"protocol":1', '"protocol":0,"protocol":1'),
      raw.replace('"sourceSha":', '"sourceSha":"' + "f".repeat(40) + '","sourceSha":'),
      JSON.stringify(evidence, null, 2) + "\n",
      raw.slice(0, -1),
      " ".repeat(JONES_PREPARATION_BOUNDS.proofBytes + 1),
    ])
      expect(() => readJonesPreparationArtifactEvidence(invalid, identity)).toThrow();
    const bundle = createJonesLegacyBootstrapPreparationBundle(
      identity,
      preparationNativeFixture(),
      evidence,
    );
    const proof = String(bundle.files[2]!.content);
    for (const invalid of [
      proof.replace('"schema":3', '"schema":2,"schema":3'),
      proof.replace('"protocol":1', '"protocol":99'),
      proof.replace('"sourceTree":"' + identity.sourceTree, '"sourceTree":"' + "f".repeat(40)),
      proofJson({ ...JSON.parse(proof), path: "/caller/runtime" }),
      proof + " ".repeat(JONES_PREPARATION_BOUNDS.proofBytes),
    ])
      expect(() => readJonesLegacyBootstrapPreparationProof(invalid, identity)).toThrow();
  });

  it("rejects any executable member other than the fixed retained worker", () => {
    const evidence = preparationFixture();
    expect(() =>
      readJonesPreparationArtifactEvidence(
        proofJson({
          ...evidence,
          preparation: {
            ...evidence.preparation,
            executable: {
              ...evidence.preparation.executable,
              name: "../node",
            },
          },
        }),
        identity,
      ),
    ).toThrow();
  });

  it("revalidates typed producer records rather than treating their TypeScript shape as proof", () => {
    const evidence = preparationFixture();
    Object.assign(evidence, { classification: "held-entry-only" });
    expect(() =>
      createJonesLegacyBootstrapPreparationBundle(identity, preparationNativeFixture(), evidence),
    ).toThrow();
  });
});
