import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import {
  createJonesLegacyBootstrapBundle,
  readJonesLegacyBootstrapNativeProof,
} from "./legacy-bootstrap-bundle.ts";

const v1ResourceNames = [
  "jones-bootstrap/bootstrap.py",
  "jones-bootstrap/executor.py",
  "jones-bootstrap/BUNDLE.json",
];

function identity(info) {
  return [info.dev, info.ino, info.uid, info.gid, info.nlink, info.size, info.mtimeNs];
}

function unchanged(actual, expected) {
  NodeAssert.deepEqual(identity(actual), identity(expected), "Bootstrap file identity changed.");
  NodeAssert.equal(actual.mode, expected.mode, "Bootstrap file mode changed during validation.");
  NodeAssert.equal(actual.ctimeNs, expected.ctimeNs, "Bootstrap file changed during validation.");
}

async function verifyBytes(handle, content, name) {
  const expected = Buffer.from(content);
  const actual = Buffer.alloc(expected.length);
  let offset = 0;
  while (offset < actual.length) {
    const { bytesRead } = await handle.read(actual, offset, actual.length - offset, offset);
    NodeAssert.ok(bytesRead > 0, `Bootstrap source bytes changed: ${name}`);
    offset += bytesRead;
  }
  const extra = await handle.read(Buffer.alloc(1), 0, 1, offset);
  NodeAssert.ok(
    extra.bytesRead === 0 && actual.equals(expected),
    `Bootstrap source bytes changed: ${name}`,
  );
}

// electron-builder 26.15.6 copies extraResources before afterPack and signs /
// emits distributables afterwards. Its builder-util 26.15.3 copier broadens
// owner-only read/execute bits; restore only the verified fixed resource set.
export default async function legacyBootstrapAfterPack(
  context,
  nativeProofRaw = process.env.JONES_LEGACY_BOOTSTRAP_NATIVE_PROOF,
) {
  NodeAssert.equal(context?.electronPlatformName, "darwin", "Bootstrap hook requires Darwin.");
  const metadata = context.packager?.info?.metadata;
  const originalMetadata = context.packager?.info?.originalMetadata;
  const configuration = context.packager?.info?.config;
  const appInfo = context.packager?.appInfo;
  NodeAssert.equal(metadata?.startupGateProtocol, 1, "Bootstrap hook requires startup protocol 1.");
  NodeAssert.equal(typeof metadata?.version, "string", "Bootstrap build version is missing.");
  NodeAssert.equal(appInfo?.version, metadata.version, "Bootstrap build versions differ.");
  NodeAssert.ok(
    configuration && typeof configuration === "object",
    "Bootstrap builder config is missing.",
  );
  for (const key of ["version", "jonesSource", "startupGateProtocol"]) {
    NodeAssert.ok(
      !Object.hasOwn(configuration.extraMetadata ?? {}, key),
      "Bootstrap staged metadata overrides are forbidden.",
    );
    NodeAssert.deepEqual(
      metadata[key],
      originalMetadata?.[key],
      "Bootstrap staged metadata differs.",
    );
  }
  const source = metadata.jonesSource;
  NodeAssert.ok(
    source && typeof source.sha === "string" && typeof source.tree === "string",
    "Bootstrap build provenance is missing.",
  );
  const bundleIdentity = {
    repository: source.repository,
    sourceSha: source.sha,
    sourceTree: source.tree,
    version: metadata.version,
  };
  const nativeProof =
    nativeProofRaw === undefined
      ? undefined
      : readJonesLegacyBootstrapNativeProof(nativeProofRaw, bundleIdentity);
  const resourceNames =
    nativeProof === undefined
      ? v1ResourceNames
      : [...v1ResourceNames, "jones-bootstrap/maintenance.py", "jones-bootstrap/identity-native"];
  const product = appInfo.productFilename;
  NodeAssert.equal(product, "Jones Code", "Bootstrap product filename is invalid.");
  const projectDir = context.packager.info.projectDir;
  for (const directory of [projectDir, context.outDir, context.appOutDir]) {
    NodeAssert.ok(typeof directory === "string" && NodePath.isAbsolute(directory));
    NodeAssert.equal(
      await NodeFSP.realpath(directory),
      directory,
      "Bootstrap output must be canonical.",
    );
  }
  NodeAssert.equal(
    context.outDir,
    NodePath.join(projectDir, "dist"),
    "Bootstrap output is outside staging.",
  );
  NodeAssert.equal(NodePath.dirname(context.appOutDir), context.outDir);
  const owner = BigInt(NodeOS.userInfo().uid);
  const directories = [projectDir, context.outDir, context.appOutDir];
  for (const component of [`${product}.app`, "Contents", "Resources", "jones-bootstrap"])
    directories.push(NodePath.join(directories.at(-1), component));
  const directoryProofs = [];
  for (const directory of directories) {
    const info = await NodeFSP.lstat(directory, { bigint: true });
    NodeAssert.ok(
      info.isDirectory() && info.uid === owner && (info.mode & 0o022n) === 0n,
      "Bootstrap output directory is not an owned, non-writable-by-others directory.",
    );
    directoryProofs.push({ directory, info });
  }
  const verifyDirectories = async () => {
    for (const { directory, info } of directoryProofs) {
      const current = await NodeFSP.lstat(directory, { bigint: true });
      NodeAssert.deepEqual(
        [current.dev, current.ino, current.uid, current.mode],
        [info.dev, info.ino, info.uid, info.mode],
        "Bootstrap output directory changed.",
      );
    }
    NodeAssert.deepEqual(
      (await NodeFSP.readdir(directories.at(-1))).sort(),
      resourceNames.map((name) => NodePath.basename(name)).sort(),
      "Bootstrap bundle contains unexpected entries.",
    );
  };
  const resources = directories.at(-2);
  await verifyDirectories();
  let files;
  if (nativeProof === undefined) {
    files = createJonesLegacyBootstrapBundle(bundleIdentity);
  } else {
    const binaryPath = NodePath.join(resources, "jones-bootstrap/identity-native");
    const before = await NodeFSP.lstat(binaryPath, { bigint: true });
    NodeAssert.ok(
      before.isFile() && before.uid === owner && before.nlink === 1n && before.size <= 4194304n,
    );
    const binaryHandle = await NodeFSP.open(
      binaryPath,
      NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW | NodeFS.constants.O_NONBLOCK,
    );
    let binary;
    try {
      unchanged(await binaryHandle.stat({ bigint: true }), before);
      binary = Buffer.alloc(Number(before.size));
      let offset = 0;
      while (offset < binary.length) {
        const { bytesRead } = await binaryHandle.read(
          binary,
          offset,
          binary.length - offset,
          offset,
        );
        NodeAssert.ok(bytesRead > 0, "Native resource was truncated.");
        offset += bytesRead;
      }
      NodeAssert.equal(
        (await binaryHandle.read(Buffer.alloc(1), 0, 1, offset)).bytesRead,
        0,
        "Native resource grew during verification.",
      );
      unchanged(await binaryHandle.stat({ bigint: true }), before);
      unchanged(await NodeFSP.lstat(binaryPath, { bigint: true }), before);
    } finally {
      await binaryHandle.close();
    }
    const digest = (bytes) => NodeCrypto.createHash("sha256").update(bytes).digest("hex");
    NodeAssert.equal(
      digest(binary),
      nativeProof.identityNativeSha256,
      "Native build bytes changed after compilation/signing.",
    );
    const nativeSource = await NodeFSP.readFile(
      new URL(
        "../../../apps/desktop/src/jones/updates/jonesLegacyIdentityNative.c",
        import.meta.url,
      ),
    );
    NodeAssert.equal(
      digest(nativeSource),
      nativeProof.identityNativeSourceSha256,
      "Native source differs from the build proof.",
    );
    const { jonesLegacyMaintenanceSource } =
      await import("../../../apps/desktop/src/jones/updates/jonesLegacyMaintenanceSource.ts");
    files = createJonesLegacyBootstrapBundle(bundleIdentity, {
      identityNative: binary,
      identityNativeSourceSha256: digest(nativeSource),
      maintenanceSource: jonesLegacyMaintenanceSource,
    });
    NodeAssert.deepEqual(
      JSON.parse(files[2].content),
      nativeProof,
      "Native build proof and packaged sources differ.",
    );
  }
  NodeAssert.deepEqual(
    files.map((file) => file.relativePath),
    resourceNames,
  );
  const opened = [];
  let failure;
  try {
    for (const file of files) {
      const filename = NodePath.join(resources, file.relativePath);
      const before = await NodeFSP.lstat(filename, { bigint: true });
      NodeAssert.ok(
        before.isFile() && before.uid === owner && before.nlink === 1n,
        `Bootstrap resource must be an owned regular single-link file: ${file.relativePath}`,
      );
      const mode = Number(before.mode & 0o7777n);
      const copiedMode = file.mode === 0o500 ? 0o555 : 0o444;
      NodeAssert.ok(
        mode === file.mode || mode === copiedMode,
        `Bootstrap resource has an unrecognized mode: ${file.relativePath}`,
      );
      NodeAssert.equal(
        before.size,
        BigInt(Buffer.byteLength(file.content)),
        `Bootstrap source bytes changed: ${file.relativePath}`,
      );
      const handle = await NodeFSP.open(
        filename,
        NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW | NodeFS.constants.O_NONBLOCK,
      );
      const record = { handle, filename, file, before };
      opened.push(record);
      unchanged(await handle.stat({ bigint: true }), before);
      await verifyBytes(handle, file.content, file.relativePath);
      unchanged(await handle.stat({ bigint: true }), before);
    }
    // Validate the entire bundle before changing any mode. Descriptor chmod
    // cannot be redirected to a replacement pathname after validation.
    await verifyDirectories();
    for (const { handle, filename, before } of opened) {
      unchanged(await handle.stat({ bigint: true }), before);
      unchanged(await NodeFSP.lstat(filename, { bigint: true }), before);
    }
    for (const record of opened) {
      const { handle, filename, file, before } = record;
      unchanged(await NodeFSP.lstat(filename, { bigint: true }), before);
      unchanged(await handle.stat({ bigint: true }), before);
      if (Number(before.mode & 0o7777n) !== file.mode) await handle.chmod(file.mode);
      const after = await handle.stat({ bigint: true });
      NodeAssert.deepEqual(
        identity(after),
        identity(before),
        "Bootstrap identity changed during chmod.",
      );
      NodeAssert.equal(Number(after.mode & 0o7777n), file.mode);
      await verifyBytes(handle, file.content, file.relativePath);
      unchanged(await handle.stat({ bigint: true }), after);
      record.verified = after;
    }
    await verifyDirectories();
    for (const { handle, filename, verified } of opened) {
      unchanged(await handle.stat({ bigint: true }), verified);
      unchanged(await NodeFSP.lstat(filename, { bigint: true }), verified);
    }
  } catch (error) {
    failure = error;
  } finally {
    const results = await Promise.allSettled(opened.map(({ handle }) => handle.close()));
    const closeErrors = results.flatMap((result) =>
      result.status === "rejected" ? [result.reason] : [],
    );
    if (closeErrors.length) {
      failure = new AggregateError(
        failure === undefined ? closeErrors : [failure, ...closeErrors],
        "Bootstrap verification or descriptor cleanup failed.",
        { cause: failure },
      );
    }
  }
  if (failure !== undefined) throw failure;
}
