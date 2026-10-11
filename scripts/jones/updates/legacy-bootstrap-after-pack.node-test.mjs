import * as Context from "effect/Context";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
import legacyBootstrapAfterPack, {
  copyJonesPreparationRuntime,
  verifyJonesPreparationRuntime,
} from "./legacy-bootstrap-after-pack.mjs";
import {
  createJonesLegacyBootstrapBundle,
  JONES_PREPARATION_SOURCE_INPUTS,
} from "./legacy-bootstrap-bundle.ts";
import { FixtureOwnership } from "./native-startup-fixture.mjs";

const posix = {
  skip: !["linux", "darwin"].includes(Context.get(Context.empty(), HostProcessPlatform)),
};

async function fixture(test, withNative = false) {
  const ownership = new FixtureOwnership();
  const allocated = await ownership.temporary(NodePath.join(NodeOS.tmpdir(), "jones-after-pack-"));
  test.after(async () => {
    await ownership.cleanup();
    await NodeAssert.rejects(NodeFSP.lstat(allocated), { code: "ENOENT" });
  });
  const root = await NodeFSP.realpath(allocated);
  const outDir = NodePath.join(root, "dist");
  const appOutDir = NodePath.join(outDir, "mac-arm64");
  const resources = NodePath.join(appOutDir, "Jones Code.app", "Contents", "Resources");
  await NodeFSP.mkdir(NodePath.join(resources, "jones-bootstrap"), {
    recursive: true,
    mode: 0o700,
  });
  const metadata = {
    version: "1.0.0-preview.20261011.1.1",
    startupGateProtocol: 1,
    jonesSource: {
      repository: "Jones-Systems/Jones-Code",
      sha: "a".repeat(40),
      tree: "b".repeat(40),
    },
  };
  const identity = {
    repository: metadata.jonesSource.repository,
    sourceSha: metadata.jonesSource.sha,
    sourceTree: metadata.jonesSource.tree,
    version: metadata.version,
  };
  let files = createJonesLegacyBootstrapBundle(identity);
  if (withNative) {
    const binary = Buffer.alloc(160);
    [0xfeedfacf, 0x0100000c, 0, 2, 2, 96, 0, 0].forEach((value, index) =>
      binary.writeUInt32LE(value, index * 4),
    );
    binary.writeUInt32LE(0x19, 32);
    binary.writeUInt32LE(72, 36);
    binary.write("__TEXT", 40, "ascii");
    binary.writeBigUInt64LE(160n, 80);
    binary.writeUInt32LE(0x80000028, 104);
    binary.writeUInt32LE(24, 108);
    binary.writeBigUInt64LE(128n, 112);
    const source = await NodeFSP.readFile(
      new URL(
        "../../../apps/desktop/src/jones/updates/jonesLegacyIdentityNative.c",
        import.meta.url,
      ),
    );
    const { jonesLegacyMaintenanceSource } =
      await import("../../../apps/desktop/src/jones/updates/jonesLegacyMaintenanceSource.ts");
    files = createJonesLegacyBootstrapBundle(identity, {
      identityNative: binary,
      identityNativeSourceSha256: NodeCrypto.createHash("sha256").update(source).digest("hex"),
      maintenanceSource: jonesLegacyMaintenanceSource,
    });
  }
  let runtime;
  let runtimeBytes;
  if (withNative === "preparation") {
    // Inert bytes and synthetic proof data exercise packaging, never worker qualification.
    runtimeBytes = Buffer.alloc(5 * 1024 * 1024 + 17, 0x5a);
    const hash = (value) => NodeCrypto.createHash("sha256").update(value).digest("hex");
    const config =
      '{"main":"offlinePreparationEntry.mjs","mainFormat":"module","executable":"node","output":"jones-offline-preparation","disableExperimentalSEAWarning":true,"useSnapshot":false,"useCodeCache":false,"execArgv":[],"execArgvExtension":"none"}\n';
    const preparation = {
      schema: 1,
      protocol: 1,
      provenanceSha256: "1".repeat(64),
      sourceInputHashes: Object.fromEntries(
        JONES_PREPARATION_SOURCE_INPUTS.map((name) => [name, "2".repeat(64)]),
      ),
      entry: { sha256: "3".repeat(64), size: 4096 },
      seaConfig: { sha256: hash(config), size: Buffer.byteLength(config) },
      executable: {
        name: "jones-offline-preparation",
        sha256: hash(runtimeBytes),
        size: runtimeBytes.length,
      },
      node: {
        version: "26.8.2",
        platform: "darwin",
        architecture: "arm64",
        archiveSha256: "4".repeat(64),
        archiveBytes: 4096,
        checksumsSha256: "5".repeat(64),
        runtimeSha256: "6".repeat(64),
        runtimeBytes: runtimeBytes.length,
      },
      sqlite: { version: "3.50.4", sourceId: "2025-07-30 19:33:53 " + "7".repeat(64) },
    };
    files = files.map((file) =>
      file.relativePath.endsWith("BUNDLE.json")
        ? {
            ...file,
            content: JSON.stringify({ ...JSON.parse(file.content), schema: 3, preparation }) + "\n",
          }
        : file,
    );
    runtime = {
      kind: "streamed-file",
      relativePath: "jones-bootstrap/jones-offline-preparation",
      mode: 0o500,
      sha256: hash(runtimeBytes),
      size: runtimeBytes.length,
      maxBytes: 256 * 1024 * 1024,
    };
    await NodeFSP.writeFile(NodePath.join(resources, runtime.relativePath), runtimeBytes, {
      flag: "wx",
      mode: 0o555,
    });
  }
  const nativeProof = withNative ? files[2].content : undefined;
  for (const file of files) {
    const filename = NodePath.join(resources, file.relativePath);
    await NodeFSP.writeFile(filename, file.content, { flag: "wx", mode: 0o600 });
    await NodeFSP.chmod(filename, file.mode === 0o500 ? 0o555 : 0o444);
  }
  const context = {
    electronPlatformName: "darwin",
    outDir,
    appOutDir,
    packager: {
      info: { metadata, originalMetadata: structuredClone(metadata), projectDir: root, config: {} },
      appInfo: { version: metadata.version, productFilename: "Jones Code" },
    },
  };
  return { root, resources, files, context, nativeProof, runtime, runtimeBytes };
}

async function modes(resources, files) {
  return Promise.all(
    files.map(
      async (file) =>
        (await NodeFSP.lstat(NodePath.join(resources, file.relativePath))).mode & 0o7777,
    ),
  );
}

NodeTest.test(
  "v3 validates all six resources before restoring modes and streams a runtime larger than the C limit",
  posix,
  async (test) => {
    const { resources, files, context, nativeProof, runtime } = await fixture(test, "preparation");
    const filename = NodePath.join(resources, runtime.relativePath);
    const bad = JSON.parse(nativeProof);
    bad.preparation.executable.sha256 = "0".repeat(64);
    await NodeAssert.rejects(legacyBootstrapAfterPack(context, JSON.stringify(bad) + "\n"));
    NodeAssert.deepEqual(await modes(resources, files), [0o555, 0o555, 0o444, 0o555, 0o555]);
    NodeAssert.equal((await NodeFSP.stat(filename)).mode & 0o7777, 0o555);
    await legacyBootstrapAfterPack(context, nativeProof);
    NodeAssert.deepEqual(await modes(resources, files), [0o500, 0o500, 0o400, 0o500, 0o500]);
    await verifyJonesPreparationRuntime(filename, runtime);
    await legacyBootstrapAfterPack(context, nativeProof);
    await NodeAssert.rejects(legacyBootstrapAfterPack(context, nativeProof.trimEnd()));
  },
);

NodeTest.test(
  "v3 refuses changed signed bytes, wrong size, invalid modes and unknown entries",
  posix,
  async (test) => {
    const { resources, context, nativeProof, runtime } = await fixture(test, "preparation");
    const filename = NodePath.join(resources, runtime.relativePath);
    await legacyBootstrapAfterPack(context, nativeProof);
    await NodeAssert.rejects(
      verifyJonesPreparationRuntime(filename, { ...runtime, size: runtime.size - 1 }),
    );
    await NodeAssert.rejects(
      verifyJonesPreparationRuntime(filename, { ...runtime, size: 256 * 1024 * 1024 + 1 }),
    );
    await NodeFSP.chmod(filename, 0o700);
    await NodeAssert.rejects(legacyBootstrapAfterPack(context, nativeProof));
    await NodeFSP.chmod(filename, 0o500);
    await NodeFSP.writeFile(NodePath.join(resources, "jones-bootstrap/extra"), "retained", {
      flag: "wx",
    });
    await NodeAssert.rejects(legacyBootstrapAfterPack(context, nativeProof));
    await NodeFSP.unlink(NodePath.join(resources, "jones-bootstrap/extra"));
    await NodeFSP.chmod(filename, 0o600);
    const handle = await NodeFSP.open(filename, "r+");
    try {
      await handle.write(Buffer.from([0]), 0, 1, 0);
    } finally {
      await handle.close();
    }
    await NodeFSP.chmod(filename, 0o500);
    await NodeAssert.rejects(legacyBootstrapAfterPack(context, nativeProof), /digest/);
  },
);

NodeTest.test(
  "runtime copy is exclusive, byte exact, and retains failed copies",
  posix,
  async (test) => {
    const { root, resources, context, nativeProof, runtime, runtimeBytes } = await fixture(
      test,
      "preparation",
    );
    await legacyBootstrapAfterPack(context, nativeProof);
    const source = NodePath.join(resources, runtime.relativePath);
    const parent = NodePath.join(root, "copy/jones-bootstrap");
    await NodeFSP.mkdir(parent, { recursive: true, mode: 0o700 });
    const destination = NodePath.join(parent, "jones-offline-preparation");
    await copyJonesPreparationRuntime(source, destination, runtime);
    NodeAssert.deepEqual(await NodeFSP.readFile(destination), runtimeBytes);
    NodeAssert.equal((await NodeFSP.stat(destination)).mode & 0o7777, 0o500);
    await NodeAssert.rejects(copyJonesPreparationRuntime(source, destination, runtime), {
      code: "EEXIST",
    });
    const failedParent = NodePath.join(root, "failed/jones-bootstrap");
    await NodeFSP.mkdir(failedParent, { recursive: true, mode: 0o700 });
    const failed = NodePath.join(failedParent, "jones-offline-preparation");
    await NodeAssert.rejects(
      copyJonesPreparationRuntime(source, failed, { ...runtime, sha256: "0".repeat(64) }),
      /digest/,
    );
    NodeAssert.equal((await NodeFSP.stat(failed)).size, runtime.size);
  },
);

NodeTest.test("runtime verification refuses links and aliased parents", posix, async (test) => {
  const { root, resources, context, nativeProof, runtime } = await fixture(test, "preparation");
  await legacyBootstrapAfterPack(context, nativeProof);
  const source = NodePath.join(resources, runtime.relativePath);
  const alias = NodePath.join(root, "runtime-link");
  await NodeFSP.symlink(source, alias);
  await NodeAssert.rejects(verifyJonesPreparationRuntime(alias, runtime));
  await NodeFSP.unlink(alias);
  await NodeFSP.link(source, alias);
  await NodeAssert.rejects(verifyJonesPreparationRuntime(source, runtime));
  await NodeFSP.unlink(alias);
  const directoryAlias = NodePath.join(root, "directory-link");
  await NodeFSP.symlink(NodePath.dirname(source), directoryAlias);
  await NodeAssert.rejects(
    verifyJonesPreparationRuntime(
      NodePath.join(directoryAlias, NodePath.basename(source)),
      runtime,
    ),
    /parent/,
  );
});

NodeTest.test(
  "afterPack restores copied modes without changing source bytes and is repeatable",
  posix,
  async (test) => {
    const { resources, files, context } = await fixture(test);
    await legacyBootstrapAfterPack(context);
    NodeAssert.deepEqual(await modes(resources, files), [0o500, 0o500, 0o400]);
    for (const file of files)
      NodeAssert.equal(
        await NodeFSP.readFile(NodePath.join(resources, file.relativePath), "utf8"),
        file.content,
      );
    await legacyBootstrapAfterPack(context);
    NodeAssert.deepEqual(await modes(resources, files), [0o500, 0o500, 0o400]);
  },
);

NodeTest.test(
  "afterPack refuses a late bundle tamper before changing earlier file modes",
  posix,
  async (test) => {
    const { resources, files, context } = await fixture(test);
    const manifest = NodePath.join(resources, files[2].relativePath);
    await NodeFSP.chmod(manifest, 0o600);
    try {
      await NodeFSP.writeFile(manifest, files[2].content.replace('"schema":1', '"schema":2'));
    } finally {
      await NodeFSP.chmod(manifest, 0o444);
    }
    await NodeAssert.rejects(
      legacyBootstrapAfterPack(context),
      /Bootstrap source bytes changed: jones-bootstrap\/BUNDLE.json/,
    );
    NodeAssert.deepEqual(await modes(resources, files), [0o555, 0o555, 0o444]);
  },
);

NodeTest.test(
  "afterPack derives provenance from the build rather than the copied manifest",
  posix,
  async (test) => {
    const { resources, files, context } = await fixture(test);
    context.packager.info.metadata.jonesSource.sha = "c".repeat(40);
    context.packager.info.originalMetadata.jonesSource.sha = "c".repeat(40);
    await NodeAssert.rejects(
      legacyBootstrapAfterPack(context),
      /Bootstrap source bytes changed: jones-bootstrap\/BUNDLE.json/,
    );
    NodeAssert.deepEqual(await modes(resources, files), [0o555, 0o555, 0o444]);
  },
);

NodeTest.test(
  "afterPack rejects modes outside the declared and pinned copier modes",
  posix,
  async (test) => {
    const { resources, files, context } = await fixture(test);
    const executor = NodePath.join(resources, files[1].relativePath);
    await NodeFSP.chmod(executor, 0o755);
    await NodeAssert.rejects(
      legacyBootstrapAfterPack(context),
      /Bootstrap resource has an unrecognized mode/,
    );
    NodeAssert.deepEqual(await modes(resources, files), [0o555, 0o755, 0o444]);
  },
);

NodeTest.test(
  "afterPack refuses a symlink resource without modifying its target",
  posix,
  async (test) => {
    const { root, resources, files, context } = await fixture(test);
    const bootstrap = NodePath.join(resources, files[0].relativePath);
    const target = NodePath.join(root, "owned-target.py");
    await NodeFSP.rename(bootstrap, target);
    await NodeFSP.symlink(target, bootstrap);
    await NodeAssert.rejects(legacyBootstrapAfterPack(context), /owned regular single-link file/);
    NodeAssert.equal((await NodeFSP.stat(target)).mode & 0o7777, 0o555);
    NodeAssert.equal(await NodeFSP.readFile(target, "utf8"), files[0].content);
  },
);

NodeTest.test(
  "afterPack refuses a hard-linked resource without chmodding either link",
  posix,
  async (test) => {
    const { root, resources, files, context } = await fixture(test);
    const executor = NodePath.join(resources, files[1].relativePath);
    const alias = NodePath.join(root, "owned-alias.py");
    await NodeFSP.link(executor, alias);
    await NodeAssert.rejects(legacyBootstrapAfterPack(context), /owned regular single-link file/);
    NodeAssert.equal((await NodeFSP.stat(alias)).mode & 0o7777, 0o555);
    NodeAssert.deepEqual(await modes(resources, files), [0o555, 0o555, 0o444]);
  },
);

NodeTest.test(
  "afterPack refuses a symlink bundle directory before accessing its files",
  posix,
  async (test) => {
    const { root, resources, files, context } = await fixture(test);
    const directory = NodePath.join(resources, "jones-bootstrap");
    const target = NodePath.join(root, "owned-bundle");
    await NodeFSP.rename(directory, target);
    await NodeFSP.symlink(target, directory);
    await NodeAssert.rejects(legacyBootstrapAfterPack(context), /Bootstrap output directory/);
    NodeAssert.equal(
      (await NodeFSP.stat(NodePath.join(target, "bootstrap.py"))).mode & 0o7777,
      0o555,
    );
    NodeAssert.equal(
      await NodeFSP.readFile(NodePath.join(target, "bootstrap.py"), "utf8"),
      files[0].content,
    );
  },
);

NodeTest.test(
  "afterPack rejects wrong platform, build version, product, and missing provenance",
  posix,
  async (test) => {
    const { resources, files, context } = await fixture(test);
    const cases = [
      [
        () => {
          context.electronPlatformName = "linux";
        },
        /requires Darwin/,
      ],
      [
        () => {
          context.electronPlatformName = "darwin";
          context.packager.appInfo.version = "wrong";
        },
        /build versions differ/,
      ],
      [
        () => {
          context.packager.appInfo.version = context.packager.info.metadata.version;
          context.packager.appInfo.productFilename = "../Jones Code";
        },
        /product filename is invalid/,
      ],
      [
        () => {
          context.packager.appInfo.productFilename = "Jones Code";
          delete context.packager.info.metadata.jonesSource;
          delete context.packager.info.originalMetadata.jonesSource;
        },
        /provenance is missing/,
      ],
    ];
    for (const [change, expected] of cases) {
      change();
      await NodeAssert.rejects(legacyBootstrapAfterPack(context), expected);
    }
    NodeAssert.deepEqual(await modes(resources, files), [0o555, 0o555, 0o444]);
  },
);

NodeTest.test(
  "afterPack rejects merged metadata overrides and output outside the staged project",
  posix,
  async (test) => {
    const { root, resources, files, context } = await fixture(test);
    context.packager.info.originalMetadata.jonesSource.sha = "c".repeat(40);
    await NodeAssert.rejects(legacyBootstrapAfterPack(context), /staged metadata differs/);
    context.packager.info.originalMetadata = structuredClone(context.packager.info.metadata);
    context.packager.info.config.extraMetadata = {
      version: context.packager.info.metadata.version,
    };
    await NodeAssert.rejects(legacyBootstrapAfterPack(context), /metadata overrides are forbidden/);
    delete context.packager.info.config.extraMetadata;
    context.packager.info.projectDir = context.outDir;
    await NodeAssert.rejects(legacyBootstrapAfterPack(context), /output is outside staging/);
    context.packager.info.projectDir = root;
    await NodeFSP.writeFile(NodePath.join(resources, "jones-bootstrap", "unexpected"), "fixture", {
      flag: "wx",
    });
    await NodeAssert.rejects(
      legacyBootstrapAfterPack(context),
      /bundle contains unexpected entries/,
    );
    NodeAssert.deepEqual(await modes(resources, files), [0o555, 0o555, 0o444]);
  },
);

NodeTest.test(
  "v2 afterPack binds all five resources to the independent build record",
  posix,
  async (test) => {
    const { resources, files, context, nativeProof } = await fixture(test, true);
    await legacyBootstrapAfterPack(context, nativeProof);
    NodeAssert.deepEqual(await modes(resources, files), [0o500, 0o500, 0o400, 0o500, 0o500]);
    for (const file of files)
      NodeAssert.deepEqual(
        await NodeFSP.readFile(NodePath.join(resources, file.relativePath)),
        Buffer.from(file.content),
      );
    await legacyBootstrapAfterPack(context, nativeProof);
  },
);

NodeTest.test(
  "matching copied native and manifest tampering cannot replace independent build proof",
  posix,
  async (test) => {
    const { resources, files, context, nativeProof } = await fixture(test, true);
    const binary = Buffer.from(files[4].content);
    binary[159] = 255;
    const manifest = JSON.parse(nativeProof);
    manifest.identityNativeSha256 = NodeCrypto.createHash("sha256").update(binary).digest("hex");
    for (const [index, content] of [
      [4, binary],
      [2, JSON.stringify(manifest) + "\n"],
    ]) {
      const filename = NodePath.join(resources, files[index].relativePath);
      await NodeFSP.chmod(filename, 0o600);
      await NodeFSP.writeFile(filename, content);
      await NodeFSP.chmod(filename, index === 2 ? 0o444 : 0o555);
    }
    await NodeAssert.rejects(
      legacyBootstrapAfterPack(context, nativeProof),
      /Native build bytes changed/,
    );
    NodeAssert.deepEqual(await modes(resources, files), [0o555, 0o555, 0o444, 0o555, 0o555]);
  },
);

NodeTest.test(
  "v2 afterPack refuses missing proof, extensions and changed source identity before chmod",
  posix,
  async (test) => {
    const { resources, files, context, nativeProof } = await fixture(test, true);
    await NodeAssert.rejects(legacyBootstrapAfterPack(context), /unexpected entries/);
    const proof = JSON.parse(nativeProof);
    await NodeAssert.rejects(
      legacyBootstrapAfterPack(context, JSON.stringify({ ...proof, command: "untrusted" })),
      /exact source and architecture/,
    );
    await NodeAssert.rejects(
      legacyBootstrapAfterPack(
        context,
        JSON.stringify({ ...proof, identityNativeSourceSha256: "a".repeat(64) }),
      ),
      /Native source differs/,
    );
    NodeAssert.deepEqual(await modes(resources, files), [0o555, 0o555, 0o444, 0o555, 0o555]);
  },
);
