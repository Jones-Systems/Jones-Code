import * as NodeAssert from "node:assert/strict";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import {
  createJonesLegacyBootstrapBundle,
  readJonesLegacyBootstrapNativeProof,
  readJonesLegacyBootstrapPreparationProof,
  createJonesLegacyBootstrapPreparationBundle,
  JONES_PREPARATION_BOUNDS,
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

export function readPackagedBootstrapProof(raw, bundleIdentity) {
  NodeAssert.ok(
    typeof raw === "string" && Buffer.byteLength(raw) <= JONES_PREPARATION_BOUNDS.proofBytes,
  );
  return JSON.parse(raw).schema === 3
    ? readJonesLegacyBootstrapPreparationProof(raw, bundleIdentity)
    : readJonesLegacyBootstrapNativeProof(raw, bundleIdentity);
}

// Reconstruct expected resource bytes from the independently retained build proof.
// Decoding or reconstruction does not qualify a preparation producer.
export function reconstructBootstrapResources(bundleIdentity, native, proof) {
  if (proof.schema !== 3)
    return { files: createJonesLegacyBootstrapBundle(bundleIdentity, native), runtime: undefined };
  return createJonesLegacyBootstrapPreparationBundle(bundleIdentity, native, {
    schema: 1,
    observation: "complete",
    classification: "cold-preparation-worker",
    source: bundleIdentity,
    preparation: proof.preparation,
  });
}

async function captureParents(filename) {
  NodeAssert.ok(NodePath.isAbsolute(filename) && NodePath.normalize(filename) === filename);
  const owner = BigInt(NodeOS.userInfo().uid);
  const chain = [];
  let directory = NodePath.parse(filename).root;
  const parts = NodePath.dirname(filename)
    .slice(directory.length)
    .split(NodePath.sep)
    .filter(Boolean);
  for (const part of [undefined, ...parts]) {
    if (part !== undefined) directory = NodePath.join(directory, part);
    const info = await NodeFSP.lstat(directory, { bigint: true });
    const systemSticky = info.uid === 0n && (info.mode & 0o1000n) !== 0n;
    NodeAssert.ok(
      info.isDirectory() &&
        (info.uid === owner || info.uid === 0n) &&
        ((info.mode & 0o022n) === 0n || systemSticky),
      "Runtime parent is aliased or unowned.",
    );
    chain.push({ directory, info });
  }
  NodeAssert.equal(chain.at(-1).info.uid, owner);
  return async () => {
    for (const { directory, info } of chain) {
      const current = await NodeFSP.lstat(directory, { bigint: true });
      NodeAssert.deepEqual(
        [current.dev, current.ino, current.uid, current.mode],
        [info.dev, info.ino, info.uid, info.mode],
        "Runtime parent identity changed.",
      );
    }
  };
}

function validateRuntimeDescriptor(runtime) {
  NodeAssert.equal(runtime.kind, "streamed-file");
  NodeAssert.equal(runtime.relativePath, "jones-bootstrap/jones-offline-preparation");
  NodeAssert.equal(runtime.mode, 0o500);
  NodeAssert.equal(runtime.maxBytes, JONES_PREPARATION_BOUNDS.runtimeBytes);
  NodeAssert.ok(
    Number.isSafeInteger(runtime.size) && runtime.size > 0 && runtime.size <= runtime.maxBytes,
  );
  NodeAssert.ok(
    typeof runtime.sha256 === "string" &&
      runtime.sha256.length === 64 &&
      /^[a-f0-9]{64}$/.test(runtime.sha256),
  );
}

async function streamRuntime(handle, runtime, destination) {
  const digest = NodeCrypto.createHash("sha256");
  const buffer = Buffer.alloc(1024 * 1024);
  let offset = 0;
  while (offset < runtime.size) {
    const { bytesRead } = await handle.read(
      buffer,
      0,
      Math.min(buffer.length, runtime.size - offset),
      offset,
    );
    NodeAssert.ok(bytesRead > 0, "Preparation runtime was truncated.");
    digest.update(buffer.subarray(0, bytesRead));
    if (destination) {
      let written = 0;
      while (written < bytesRead) {
        const { bytesWritten } = await destination.write(
          buffer,
          written,
          bytesRead - written,
          offset + written,
        );
        NodeAssert.ok(bytesWritten > 0, "Preparation runtime copy stalled.");
        written += bytesWritten;
      }
    }
    offset += bytesRead;
  }
  NodeAssert.equal(
    (await handle.read(buffer, 0, 1, offset)).bytesRead,
    0,
    "Preparation runtime grew.",
  );
  NodeAssert.equal(
    digest.digest("hex"),
    runtime.sha256,
    "Preparation runtime digest differs from build proof.",
  );
}

async function openRuntime(filename, runtime, copied = false) {
  validateRuntimeDescriptor(runtime);
  const verifyParents = await captureParents(filename);
  const before = await NodeFSP.lstat(filename, { bigint: true });
  NodeAssert.ok(
    before.isFile() && before.uid === BigInt(NodeOS.userInfo().uid) && before.nlink === 1n,
  );
  NodeAssert.equal(before.size, BigInt(runtime.size));
  NodeAssert.ok(
    (before.mode & 0o7777n) === 0o500n || (copied && (before.mode & 0o7777n) === 0o555n),
    "Preparation runtime mode differs.",
  );
  const handle = await NodeFSP.open(
    filename,
    NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW | NodeFS.constants.O_NONBLOCK,
  );
  try {
    unchanged(await handle.stat({ bigint: true }), before);
    await verifyParents();
  } catch (cause) {
    try {
      await handle.close();
    } catch (close) {
      throw new AggregateError([cause, close], "Runtime open and close failed.");
    }
    throw cause;
  }
  return { handle, before, verifyParents };
}

export async function verifyJonesPreparationRuntime(filename, runtime) {
  const { handle, before, verifyParents } = await openRuntime(filename, runtime);
  let failure;
  try {
    await streamRuntime(handle, runtime);
    unchanged(await handle.stat({ bigint: true }), before);
    unchanged(await NodeFSP.lstat(filename, { bigint: true }), before);
    await verifyParents();
  } catch (cause) {
    failure = cause;
  }
  try {
    await handle.close();
  } catch (close) {
    throw new AggregateError(
      failure === undefined ? [close] : [failure, close],
      "Runtime verification or close failed.",
    );
  }
  if (failure !== undefined) throw failure;
}

// Partial or uncertain copies remain in the caller's owned stage for reconciliation.
export async function copyJonesPreparationRuntime(source, destination, runtime) {
  NodeAssert.equal(NodePath.basename(source), "jones-offline-preparation");
  NodeAssert.equal(NodePath.basename(destination), "jones-offline-preparation");
  NodeAssert.equal(NodePath.basename(NodePath.dirname(destination)), "jones-bootstrap");
  const verifyDestinationParents = await captureParents(destination);
  const { handle, before, verifyParents } = await openRuntime(source, runtime);
  let output;
  let failure;
  try {
    output = await NodeFSP.open(
      destination,
      NodeFS.constants.O_WRONLY |
        NodeFS.constants.O_CREAT |
        NodeFS.constants.O_EXCL |
        NodeFS.constants.O_NOFOLLOW,
      0o500,
    );
    const created = await output.stat({ bigint: true });
    NodeAssert.ok(
      created.isFile() && created.nlink === 1n && created.uid === BigInt(NodeOS.userInfo().uid),
    );
    await streamRuntime(handle, runtime, output);
    unchanged(await handle.stat({ bigint: true }), before);
    unchanged(await NodeFSP.lstat(source, { bigint: true }), before);
    await verifyParents();
    await verifyDestinationParents();
    await output.chmod(0o500);
    await output.sync();
    const final = await output.stat({ bigint: true });
    NodeAssert.deepEqual(
      [final.dev, final.ino, final.uid, final.nlink],
      [created.dev, created.ino, created.uid, created.nlink],
    );
    unchanged(await NodeFSP.lstat(destination, { bigint: true }), final);
  } catch (cause) {
    failure = cause;
  }
  const closed = await Promise.allSettled([handle.close(), ...(output ? [output.close()] : [])]);
  const errors = closed.flatMap((result) => (result.status === "rejected" ? [result.reason] : []));
  if (errors.length)
    throw new AggregateError(
      failure === undefined ? errors : [failure, ...errors],
      "Runtime copy or close failed.",
    );
  if (failure !== undefined) throw failure;
  await verifyJonesPreparationRuntime(destination, runtime);
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
      : readPackagedBootstrapProof(nativeProofRaw, bundleIdentity);
  const resourceNames =
    nativeProof === undefined
      ? v1ResourceNames
      : [
          ...v1ResourceNames,
          "jones-bootstrap/maintenance.py",
          "jones-bootstrap/identity-native",
          ...(nativeProof.schema === 3 ? ["jones-bootstrap/jones-offline-preparation"] : []),
        ];
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
  let runtime;
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
    const reconstructed = reconstructBootstrapResources(
      bundleIdentity,
      {
        identityNative: binary,
        identityNativeSourceSha256: digest(nativeSource),
        maintenanceSource: jonesLegacyMaintenanceSource,
      },
      nativeProof,
    );
    files = reconstructed.files;
    runtime = reconstructed.runtime;
    NodeAssert.deepEqual(
      JSON.parse(files[2].content),
      nativeProof,
      "Native build proof and packaged sources differ.",
    );
  }
  NodeAssert.deepEqual(
    [...files.map((file) => file.relativePath), ...(runtime ? [runtime.relativePath] : [])],
    resourceNames,
  );
  const opened = [];
  let failure;
  try {
    if (runtime) {
      const filename = NodePath.join(resources, runtime.relativePath);
      const record = await openRuntime(filename, runtime, true);
      opened.push({ ...record, filename, file: runtime });
      await streamRuntime(record.handle, runtime);
      unchanged(await record.handle.stat({ bigint: true }), record.before);
      await record.verifyParents();
    }
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
      if (file.kind === "streamed-file") await streamRuntime(handle, file);
      else await verifyBytes(handle, file.content, file.relativePath);
      unchanged(await handle.stat({ bigint: true }), after);
      record.verified = after;
    }
    await verifyDirectories();
    for (const { handle, filename, verified } of opened) {
      unchanged(await handle.stat({ bigint: true }), verified);
      unchanged(await NodeFSP.lstat(filename, { bigint: true }), verified);
    }
    for (const record of opened) if (record.verifyParents) await record.verifyParents();
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
