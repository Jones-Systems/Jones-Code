// @effect-diagnostics nodeBuiltinImport:off
// Native adoption preserves the old setup evidence without loading the server runtime.
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import { QUALIFIED_RUNTIME_RECEIPT, qualifiedPayloadDigest } from "../cloud/qualifiedRuntime.ts";

interface PrivateSetupArtifact {
  readonly repository: string;
  readonly source: string;
  readonly tree: string;
  readonly version: string;
  readonly platform: string;
  readonly architecture: string;
  readonly artifact: string;
  readonly sha256: string;
}

export function isUnattestedPrivateSetupArtifact(artifact: PrivateSetupArtifact): boolean {
  return (
    artifact.repository === "Jones-Systems/Jones-Code" &&
    artifact.source === "d3e6f8e843a0477542380a4ec0d9ae02a1084053" &&
    artifact.tree === "250db17dd136cef960e9bb8e054acadb41fba03d" &&
    artifact.platform === "linux" &&
    artifact.architecture === "x64"
  );
}

export interface PrivateSetupPreimage {
  readonly directory: string;
  readonly path: string;
  readonly text: string;
  readonly sha256: string;
  readonly identity: string;
  readonly directoryIdentity: string;
  readonly payloadSha256: string;
  readonly version: string;
  readonly entrySha256: string;
}

const provenanceName = ".jones-provenance.json";
const payloadRoots = ["t3", "client", "node_modules", "resource-monitor"];
const digest = (bytes: string | Uint8Array) =>
  NodeCrypto.createHash("sha256").update(bytes).digest("hex");
const fail = (): never => {
  throw new Error("Private setup runtime evidence or layout differs; preserve and reconcile it.");
};
const identity = (stat: NodeFS.BigIntStats) =>
  JSON.stringify(
    [stat.dev, stat.ino, stat.uid, stat.mode, stat.nlink, stat.size, stat.mtimeNs].map(String),
  );
const directoryIdentity = (stat: NodeFS.BigIntStats) =>
  JSON.stringify([stat.dev, stat.ino, stat.uid, stat.mode].map(String));

async function readProvenance(file: string, uid: number) {
  const before = await NodeFSP.lstat(file, { bigint: true });
  if (
    !before.isFile() ||
    before.uid !== BigInt(uid) ||
    before.nlink !== 1n ||
    (before.mode & 0o077n) !== 0n ||
    before.size > 65536n
  )
    return fail();
  const fd = await NodeFSP.open(file, NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW);
  try {
    if (identity(await fd.stat({ bigint: true })) !== identity(before)) return fail();
    const bytes = await fd.readFile();
    const text = bytes.toString("utf8");
    if (
      !Buffer.from(text, "utf8").equals(bytes) ||
      identity(await fd.stat({ bigint: true })) !== identity(before) ||
      identity(await NodeFSP.lstat(file, { bigint: true })) !== identity(before)
    )
      return fail();
    return { text, sha256: digest(bytes), identity: identity(before) };
  } finally {
    await fd.close();
  }
}

async function inspectLayout(directory: string, uid: number, archived: boolean, enrolled: boolean) {
  const root = await NodeFSP.lstat(directory, { bigint: true });
  if (
    !root.isDirectory() ||
    root.uid !== BigInt(uid) ||
    (root.mode & 0o022n) !== 0n ||
    (await NodeFSP.realpath(directory)) !== directory
  )
    return fail();
  const expected = [
    ...payloadRoots,
    ".install-complete",
    ...(archived ? [] : [provenanceName]),
    ...(enrolled ? [QUALIFIED_RUNTIME_RECEIPT] : []),
  ].sort();
  if (JSON.stringify((await NodeFSP.readdir(directory)).sort()) !== JSON.stringify(expected))
    return fail();
  for (const name of expected) {
    const stat = await NodeFSP.lstat(NodePath.join(directory, name));
    if (
      stat.uid !== uid ||
      (stat.mode & 0o022) !== 0 ||
      (["client", "node_modules", "resource-monitor"].includes(name)
        ? !stat.isDirectory()
        : !stat.isFile()) ||
      (name === "t3" && (stat.mode & 0o111) === 0)
    )
      return fail();
  }
  if (
    directoryIdentity(await NodeFSP.lstat(directory, { bigint: true })) !== directoryIdentity(root)
  )
    return fail();
  return directoryIdentity(root);
}

async function entryDigest(directory: string) {
  const fd = await NodeFSP.open(
    NodePath.join(directory, "t3"),
    NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW,
  );
  try {
    const hash = NodeCrypto.createHash("sha256");
    for await (const bytes of fd.createReadStream({ autoClose: false })) hash.update(bytes);
    return hash.digest("hex");
  } finally {
    await fd.close();
  }
}

export async function inspectPrivateSetupRuntime(input: {
  readonly directory: string;
  readonly uid: number;
  readonly artifact: PrivateSetupArtifact;
  readonly entrySha256: string;
}): Promise<PrivateSetupPreimage> {
  if (!isUnattestedPrivateSetupArtifact(input.artifact)) return fail();
  const rootIdentity = await inspectLayout(input.directory, input.uid, false, false);
  const path = NodePath.join(input.directory, provenanceName);
  const file = await readProvenance(path, input.uid);
  const value: unknown = JSON.parse(file.text);
  if (typeof value !== "object" || value === null || Array.isArray(value)) return fail();
  const fields = value as Record<string, unknown>;
  const expected = {
    schema: 1,
    repository: input.artifact.repository,
    source: input.artifact.source,
    version: input.artifact.version,
    platform: input.artifact.platform,
    architecture: input.artifact.architecture,
    artifact: input.artifact.artifact,
    sha256: input.artifact.sha256,
    entrySha256: input.entrySha256,
  };
  if (
    JSON.stringify(Object.keys(fields).sort()) !== JSON.stringify(Object.keys(expected).sort()) ||
    Object.entries(expected).some(([key, expectedValue]) => fields[key] !== expectedValue) ||
    (await entryDigest(input.directory)) !== input.entrySha256 ||
    (await NodeFSP.readFile(NodePath.join(input.directory, ".install-complete"), "utf8")).trim() !==
      input.artifact.version
  )
    return fail();
  return {
    directory: input.directory,
    path,
    ...file,
    directoryIdentity: rootIdentity,
    payloadSha256: await qualifiedPayloadDigest(input.directory),
    version: input.artifact.version,
    entrySha256: input.entrySha256,
  };
}

export async function assertPrivateSetupRuntime(
  preimage: PrivateSetupPreimage,
  uid: number,
  archiveDirectory?: string,
  enrolled = false,
): Promise<void> {
  if (
    (await inspectLayout(preimage.directory, uid, archiveDirectory !== undefined, enrolled)) !==
      preimage.directoryIdentity ||
    (await qualifiedPayloadDigest(preimage.directory)) !== preimage.payloadSha256 ||
    (await entryDigest(preimage.directory)) !== preimage.entrySha256 ||
    (
      await NodeFSP.readFile(NodePath.join(preimage.directory, ".install-complete"), "utf8")
    ).trim() !== preimage.version
  )
    return fail();
  if (archiveDirectory !== undefined) {
    const archive = await NodeFSP.lstat(archiveDirectory);
    if (
      !archive.isDirectory() ||
      archive.uid !== uid ||
      (archive.mode & 0o777) !== 0o700 ||
      archive.dev !== (await NodeFSP.lstat(preimage.directory)).dev ||
      (await NodeFSP.realpath(archiveDirectory)) !== archiveDirectory ||
      JSON.stringify(await NodeFSP.readdir(archiveDirectory)) !== JSON.stringify([provenanceName])
    )
      return fail();
  }
  const file = await readProvenance(
    archiveDirectory === undefined
      ? preimage.path
      : NodePath.join(archiveDirectory, provenanceName),
    uid,
  );
  if (
    file.identity !== preimage.identity ||
    file.sha256 !== preimage.sha256 ||
    file.text !== preimage.text
  )
    return fail();
}

export async function assertPrivateSetupArchiveDestination(
  preimage: PrivateSetupPreimage,
  archiveDirectory: string,
  uid: number,
): Promise<void> {
  const parent = NodePath.dirname(archiveDirectory);
  const existing = await NodeFSP.lstat(parent).catch((error: NodeJS.ErrnoException) => {
    if (error.code !== "ENOENT") throw error;
    return undefined;
  });
  const inspected = existing ?? (await NodeFSP.lstat(NodePath.dirname(parent)));
  const inspectedPath = existing === undefined ? NodePath.dirname(parent) : parent;
  if (
    !inspected.isDirectory() ||
    inspected.uid !== uid ||
    (inspected.mode & (existing === undefined ? 0o022 : 0o077)) !== 0 ||
    (await NodeFSP.realpath(inspectedPath)) !== inspectedPath ||
    inspected.dev !== (await NodeFSP.lstat(preimage.directory)).dev
  )
    return fail();
  const occupied = await NodeFSP.lstat(archiveDirectory).then(
    () => true,
    (error: NodeJS.ErrnoException) => {
      if (error.code !== "ENOENT") throw error;
      return false;
    },
  );
  if (occupied) return fail();
}

export async function archivePrivateSetupProvenance(
  preimage: PrivateSetupPreimage,
  archiveDirectory: string,
  uid: number,
): Promise<void> {
  await assertPrivateSetupRuntime(preimage, uid);
  await assertPrivateSetupArchiveDestination(preimage, archiveDirectory, uid);
  const parent = NodePath.dirname(archiveDirectory);
  await NodeFSP.mkdir(parent, { recursive: true, mode: 0o700 });
  const parentStat = await NodeFSP.lstat(parent);
  if (
    !parentStat.isDirectory() ||
    parentStat.uid !== uid ||
    (parentStat.mode & 0o777) !== 0o700 ||
    (await NodeFSP.realpath(parent)) !== parent ||
    parentStat.dev !== (await NodeFSP.lstat(preimage.directory)).dev
  )
    return fail();
  await NodeFSP.mkdir(archiveDirectory, { mode: 0o700 });
  await assertPrivateSetupRuntime(preimage, uid);
  await NodeFSP.rename(preimage.path, NodePath.join(archiveDirectory, provenanceName));
  for (const directory of [preimage.directory, archiveDirectory, parent]) {
    const fd = await NodeFSP.open(directory, "r");
    try {
      await fd.sync();
    } finally {
      await fd.close();
    }
  }
  await assertPrivateSetupRuntime(preimage, uid, archiveDirectory);
}
