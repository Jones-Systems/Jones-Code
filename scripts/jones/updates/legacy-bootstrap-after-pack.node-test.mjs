import * as Context from "effect/Context";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeTest from "node:test";
import legacyBootstrapAfterPack from "./legacy-bootstrap-after-pack.mjs";
import { createJonesLegacyBootstrapBundle } from "./legacy-bootstrap-bundle.ts";
import { FixtureOwnership } from "./native-startup-fixture.mjs";

const posix = {
  skip: !["linux", "darwin"].includes(Context.get(Context.empty(), HostProcessPlatform)),
};

async function fixture(test) {
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
  const files = createJonesLegacyBootstrapBundle({
    repository: metadata.jonesSource.repository,
    sourceSha: metadata.jonesSource.sha,
    sourceTree: metadata.jonesSource.tree,
    version: metadata.version,
  });
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
  return { root, resources, files, context };
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
