/* oxlint-disable t3code/no-global-process-runtime -- Standalone launcher boundary uses native host identity; platform qualification also accepts injected test adapters. */
// @effect-diagnostics nodeBuiltinImport:off
// Imported by the detached launcher: keep this boundary on Node built-ins.
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeModule from "node:module";
import * as NodeSqlite from "node:sqlite";
import type { JonesStagedArtifact } from "@t3tools/shared/jonesActions";

const nodeRequire = NodeModule.createRequire(import.meta.url);

/** Integrity reads raw archives; mirrors the independently packed desktop selector. */
export function bundleFileSystem(
  versions: { readonly electron?: string | undefined } = {
    electron: process.versions["electron"],
  },
  load: (id: string) => unknown = nodeRequire,
): typeof NodeFS {
  return versions.electron === undefined ? NodeFS : (load("original-fs") as typeof NodeFS);
}

export const QUALIFIED_UPDATES_PROTOCOL = 1 as const;
export const QUALIFIED_RUNTIME_RECEIPT = ".jones-runtime-receipt.json";
const HASH = /^[a-f0-9]{64}$/;
const SOURCE = /^[a-f0-9]{40}$/;
const VERSION =
  /^(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)-preview\.[0-9]+\.[0-9]+(?:\.[0-9]+)?$/;
const HANDLE = /^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/;
const localLocks = new Set<string>();

export interface QualifiedRuntimeArtifact {
  readonly repository: "Jones-Systems/Jones-Code";
  readonly channel: "jones-main";
  readonly version: string;
  readonly sourceSha: string;
  readonly sourceTree: string;
  readonly installedSourceSha: string;
  readonly runId: number;
  readonly runAttempt: number;
  readonly artifactId: number;
  readonly workflow:
    | ".github/workflows/artifact-cli-linux.yml"
    | ".github/workflows/artifact-desktop-mac.yml";
  readonly artifactDigest: string;
  readonly archiveSha256: string;
  readonly platform: "linux" | "darwin";
  readonly architecture: "x64" | "arm64";
  /** Verified and safely extracted by the Actions transport, never a URL. */
  readonly payloadDirectory: string;
}

/** Converts the already verified transport receipt; native receipts use one normalized digest representation. */
export function qualifiedRuntimeArtifactFromJonesStage(
  staged: JonesStagedArtifact,
  payloadDirectory: string,
): QualifiedRuntimeArtifact {
  const workflow =
    staged.candidate.platform === "darwin"
      ? ".github/workflows/artifact-desktop-mac.yml"
      : ".github/workflows/artifact-cli-linux.yml";
  if (staged.candidate.workflow !== workflow)
    return blocked("invalid-artifact", "The staged workflow does not match the native platform.");
  return {
    repository: "Jones-Systems/Jones-Code",
    channel: "jones-main",
    version: staged.receipt.version,
    sourceSha: staged.candidate.source,
    sourceTree: staged.candidate.tree,
    installedSourceSha: staged.candidate.installedSource,
    runId: staged.candidate.runId,
    runAttempt: staged.candidate.runAttempt,
    artifactId: staged.candidate.artifactId,
    workflow,
    artifactDigest: `sha256:${staged.candidate.artifactDigest.replace(/^sha256:/, "")}`,
    archiveSha256: staged.receipt.sha256,
    platform: staged.candidate.platform,
    architecture: staged.candidate.architecture,
    payloadDirectory,
  };
}

export interface QualifiedRuntimeReceipt extends Omit<
  QualifiedRuntimeArtifact,
  "payloadDirectory"
> {
  readonly protocol: typeof QUALIFIED_UPDATES_PROTOCOL;
  readonly payloadSha256: string;
}

export interface QualifiedRuntimeBinding {
  readonly baseDir: string;
  readonly dbPath: string;
  readonly environmentId: string;
  readonly activeVersion: string;
  readonly activeSourceSha: string;
}

export interface StagedQualifiedRuntime {
  readonly protocol: typeof QUALIFIED_UPDATES_PROTOCOL;
  readonly stagedHandle: string;
  readonly binding: QualifiedRuntimeBinding;
  readonly receipt: QualifiedRuntimeReceipt;
}

export class QualifiedRuntimeBlockedError extends Error {
  readonly reason:
    | "bootstrap-required"
    | "binding-mismatch"
    | "invalid-artifact"
    | "integrity-mismatch"
    | "occupied-cache"
    | "busy";
  constructor(reason: QualifiedRuntimeBlockedError["reason"], message: string) {
    super(message);
    this.name = "QualifiedRuntimeBlockedError";
    this.reason = reason;
  }
}

const blocked = (reason: QualifiedRuntimeBlockedError["reason"], message: string): never => {
  throw new QualifiedRuntimeBlockedError(reason, message);
};
const record = (value: unknown): Record<string, unknown> | undefined =>
  typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;

function decodeQualifiedRuntimeReceipt(value: unknown): QualifiedRuntimeReceipt | undefined {
  const r = record(value);
  if (
    r === undefined ||
    r.protocol !== QUALIFIED_UPDATES_PROTOCOL ||
    r.repository !== "Jones-Systems/Jones-Code" ||
    r.channel !== "jones-main" ||
    (r.platform !== "linux" && r.platform !== "darwin") ||
    (r.architecture !== "x64" && r.architecture !== "arm64") ||
    (r.platform === "darwin" && r.architecture !== "arm64") ||
    (r.platform === "linux"
      ? r.workflow !== ".github/workflows/artifact-cli-linux.yml"
      : r.workflow !== ".github/workflows/artifact-desktop-mac.yml") ||
    typeof r.version !== "string" ||
    !VERSION.test(r.version)
  )
    return undefined;
  for (const key of ["sourceSha", "sourceTree", "installedSourceSha"] as const)
    if (typeof r[key] !== "string" || !SOURCE.test(r[key])) return undefined;
  for (const key of ["archiveSha256", "payloadSha256"] as const)
    if (typeof r[key] !== "string" || !HASH.test(r[key])) return undefined;
  if (typeof r.artifactDigest !== "string" || !/^sha256:[a-f0-9]{64}$/.test(r.artifactDigest))
    return undefined;
  for (const key of ["runId", "runAttempt", "artifactId"] as const)
    if (typeof r[key] !== "number" || !Number.isSafeInteger(r[key]) || r[key] < 1) return undefined;
  return r as unknown as QualifiedRuntimeReceipt;
}

export function decodeStagedQualifiedRuntime(value: unknown): StagedQualifiedRuntime | undefined {
  const r = record(value);
  const b = record(r?.binding);
  const receipt = decodeQualifiedRuntimeReceipt(r?.receipt);
  if (
    r?.protocol !== QUALIFIED_UPDATES_PROTOCOL ||
    typeof r.stagedHandle !== "string" ||
    !HANDLE.test(r.stagedHandle) ||
    receipt === undefined ||
    b === undefined
  )
    return undefined;
  for (const key of [
    "baseDir",
    "dbPath",
    "environmentId",
    "activeVersion",
    "activeSourceSha",
  ] as const)
    if (typeof b[key] !== "string" || b[key].trim() === "") return undefined;
  if (
    !NodePath.isAbsolute(String(b.baseDir)) ||
    !NodePath.isAbsolute(String(b.dbPath)) ||
    !SOURCE.test(String(b.activeSourceSha))
  )
    return undefined;
  return {
    protocol: QUALIFIED_UPDATES_PROTOCOL,
    stagedHandle: r.stagedHandle,
    binding: b as unknown as QualifiedRuntimeBinding,
    receipt,
  };
}

const runtimeDirectory = (baseDir: string, version: string) =>
  NodePath.join(baseDir, "runtime", "versions", version);
const stagedPath = (baseDir: string, handle: string) => {
  if (!HANDLE.test(handle)) return blocked("invalid-artifact", "Invalid staged update handle.");
  return NodePath.join(baseDir, "runtime", "staged-updates", `${handle}.json`);
};

async function readRegularJson(file: string): Promise<unknown> {
  const fd = await NodeFSP.open(file, NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW);
  try {
    const stat = await fd.stat();
    if (
      !stat.isFile() ||
      (stat.mode & 0o022) !== 0 ||
      stat.size > 64 * 1024 ||
      (typeof process.getuid === "function" && stat.uid !== process.getuid())
    )
      return blocked("invalid-artifact", "Update receipt is not an owner-controlled regular file.");
    return JSON.parse(await fd.readFile("utf8")) as unknown;
  } finally {
    await fd.close();
  }
}

/** Cross-process lock released by SQLite even after abrupt process death. Its inode is retained. */
export async function withQualifiedRuntimeLock<A>(
  baseDir: string,
  name: string,
  operation: () => Promise<A>,
): Promise<A> {
  const runtime = NodePath.join(baseDir, "runtime");
  await NodeFSP.mkdir(runtime, { recursive: true, mode: 0o700 });
  const lockPath = NodePath.join(runtime, `${name}.sqlite`);
  if (localLocks.has(lockPath)) return blocked("busy", "Another updater already owns this home.");
  localLocks.add(lockPath);
  let database: NodeSqlite.DatabaseSync | undefined;
  try {
    const descriptor = await NodeFSP.open(lockPath, "ax", 0o600).catch(
      (error: NodeJS.ErrnoException) => {
        if (error.code !== "EEXIST") throw error;
        return undefined;
      },
    );
    await descriptor?.close();
    const stat = await NodeFSP.lstat(lockPath);
    if (
      !stat.isFile() ||
      stat.isSymbolicLink() ||
      (stat.mode & 0o077) !== 0 ||
      (typeof process.getuid === "function" && stat.uid !== process.getuid())
    )
      return blocked("busy", "Updater lock ownership is unknown.");
    database = new NodeSqlite.DatabaseSync(lockPath);
    try {
      database.exec("PRAGMA busy_timeout=0; BEGIN EXCLUSIVE");
    } catch {
      return blocked("busy", "Another updater already owns this home.");
    }
    return await operation();
  } finally {
    try {
      database?.close();
    } finally {
      localLocks.delete(lockPath);
    }
  }
}

async function durableCreate(file: string, value: unknown): Promise<void> {
  await NodeFSP.mkdir(NodePath.dirname(file), { recursive: true, mode: 0o700 });
  const fd = await NodeFSP.open(file, "wx", 0o600);
  try {
    await fd.writeFile(`${typeof value === "string" ? value : JSON.stringify(value)}\n`);
    await fd.sync();
  } finally {
    await fd.close();
  }
  const directory = await NodeFSP.open(NodePath.dirname(file), "r");
  try {
    await directory.sync();
  } finally {
    await directory.close();
  }
}

/** Hash every file and contained symlink; reject devices, links escaping the tree and unexpected roots. */
export async function qualifiedPayloadDigest(
  directory: string,
  platform: "linux" | "darwin" = "linux",
): Promise<string> {
  const fs = bundleFileSystem().promises;
  const root = await fs.realpath(directory);
  const rootStat = await fs.lstat(directory);
  if (!rootStat.isDirectory() || rootStat.isSymbolicLink())
    return blocked("invalid-artifact", "Runtime payload must be a real directory.");
  const digest = NodeCrypto.createHash("sha256");
  let count = 0;
  let total = 0;
  const visit = async (relative: string): Promise<void> => {
    const absolute = NodePath.join(root, relative);
    const stat = await fs.lstat(absolute);
    if (++count > 100_000)
      return blocked("invalid-artifact", "Runtime payload exceeds the file limit.");
    if (stat.isSymbolicLink()) {
      const target = await fs.readlink(absolute);
      const resolved = await fs.realpath(absolute);
      if (NodePath.isAbsolute(target) || !resolved.startsWith(`${root}${NodePath.sep}`))
        return blocked("invalid-artifact", "Runtime symlink escapes its payload.");
      digest.update(`link\0${relative}\0${target}\0`);
    } else if (stat.isDirectory()) {
      digest.update(`dir\0${relative}\0`);
      for (const name of (await fs.readdir(absolute)).sort())
        await visit(NodePath.join(relative, name));
    } else if (stat.isFile()) {
      total += stat.size;
      if (total > 2 * 1024 * 1024 * 1024)
        return blocked("invalid-artifact", "Runtime payload exceeds the byte limit.");
      const hash = NodeCrypto.createHash("sha256");
      const handle = await fs.open(
        absolute,
        NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW,
      );
      try {
        for await (const bytes of handle.createReadStream()) hash.update(bytes);
      } finally {
        await handle.close();
      }
      digest.update(
        `file\0${relative}\0${stat.mode & 0o111}\0${stat.size}\0${hash.digest("hex")}\0`,
      );
    } else return blocked("invalid-artifact", "Runtime payload contains a special file.");
  };
  const roots = (await fs.readdir(root)).sort();
  for (const name of roots) {
    if (name === QUALIFIED_RUNTIME_RECEIPT || name === ".install-complete") continue;
    if (
      !(
        platform === "darwin"
          ? ["t3", "Jones Code.app"]
          : ["t3", "client", "node_modules", "resource-monitor"]
      ).includes(name)
    )
      return blocked("invalid-artifact", "Runtime payload has an unexpected root entry.");
    await visit(name);
  }
  const entry = await fs.lstat(NodePath.join(root, "t3"));
  if (!entry.isFile() || (entry.mode & 0o111) === 0)
    return blocked("invalid-artifact", "Runtime payload has no executable.");
  return digest.digest("hex");
}

export async function readQualifiedRuntimeReceipt(
  baseDir: string,
  version: string,
  host: { readonly platform: string; readonly architecture: string } = {
    platform: process.platform,
    architecture: process.arch,
  },
): Promise<QualifiedRuntimeReceipt> {
  if (!VERSION.test(version))
    return blocked(
      "bootstrap-required",
      "The active runtime has no qualified Jones preview version.",
    );
  const receipt = decodeQualifiedRuntimeReceipt(
    await readRegularJson(
      NodePath.join(runtimeDirectory(baseDir, version), QUALIFIED_RUNTIME_RECEIPT),
    ).catch(() => undefined),
  );
  if (receipt === undefined || receipt.version !== version)
    return blocked(
      "bootstrap-required",
      "The runtime must be enrolled with its qualified source and payload receipt.",
    );
  if (receipt.platform !== host.platform || receipt.architecture !== host.architecture)
    return blocked("invalid-artifact", "Runtime receipt does not match this host platform.");
  if (
    (await qualifiedPayloadDigest(runtimeDirectory(baseDir, version), receipt.platform)) !==
    receipt.payloadSha256
  )
    return blocked("integrity-mismatch", "Qualified runtime payload changed.");
  return receipt;
}

export async function currentQualifiedRuntimeBinding(
  baseDir: string,
  activeVersion: string,
  host?: { readonly platform: string; readonly architecture: string },
): Promise<QualifiedRuntimeBinding> {
  const resolvedBase = await NodeFSP.realpath(baseDir);
  const dbPath = NodePath.join(resolvedBase, "userdata", "statev2.sqlite");
  const database = await NodeFSP.lstat(dbPath);
  const environment = await NodeFSP.lstat(
    NodePath.join(resolvedBase, "userdata", "environment-id"),
  );
  if (
    !database.isFile() ||
    database.isSymbolicLink() ||
    !environment.isFile() ||
    environment.isSymbolicLink()
  )
    return blocked(
      "binding-mismatch",
      "Native state or environment identity is not a regular file.",
    );
  const environmentId = (
    await NodeFSP.readFile(NodePath.join(resolvedBase, "userdata", "environment-id"), "utf8")
  ).trim();
  if (environmentId === "")
    return blocked("binding-mismatch", "Native environment identity is missing.");
  const receipt = await readQualifiedRuntimeReceipt(resolvedBase, activeVersion, host);
  return {
    baseDir: resolvedBase,
    dbPath,
    environmentId,
    activeVersion,
    activeSourceSha: receipt.sourceSha,
  };
}

export async function stageQualifiedRuntime(input: {
  readonly artifact: QualifiedRuntimeArtifact;
  readonly binding: QualifiedRuntimeBinding;
  readonly validate: (entryPath: string) => Promise<void>;
  readonly host?: { readonly platform: string; readonly architecture: string };
}): Promise<StagedQualifiedRuntime> {
  return withQualifiedRuntimeLock(input.binding.baseDir, "qualified-runtime-lock", async () => {
    const current = await currentQualifiedRuntimeBinding(
      input.binding.baseDir,
      input.binding.activeVersion,
      input.host,
    );
    if (JSON.stringify(current) !== JSON.stringify(input.binding))
      return blocked("binding-mismatch", "The selected native environment changed before staging.");
    const { payloadDirectory, ...candidate } = input.artifact;
    const digest = await qualifiedPayloadDigest(payloadDirectory, candidate.platform);
    const host = input.host ?? { platform: process.platform, architecture: process.arch };
    const receipt = decodeQualifiedRuntimeReceipt({
      ...candidate,
      protocol: QUALIFIED_UPDATES_PROTOCOL,
      payloadSha256: digest,
    });
    if (
      receipt === undefined ||
      receipt.installedSourceSha !== current.activeSourceSha ||
      receipt.sourceSha === current.activeSourceSha ||
      receipt.platform !== host.platform ||
      receipt.architecture !== host.architecture
    )
      return blocked(
        "invalid-artifact",
        "Candidate does not bind this qualified runtime and platform.",
      );
    const destination = runtimeDirectory(current.baseDir, receipt.version);
    const occupied = await NodeFSP.lstat(destination).then(
      () => true,
      (error: NodeJS.ErrnoException) => {
        if (error.code !== "ENOENT") throw error;
        return false;
      },
    );
    if (occupied) {
      const cached = await readQualifiedRuntimeReceipt(
        current.baseDir,
        receipt.version,
        host,
      ).catch(() =>
        blocked(
          "occupied-cache",
          "The occupied runtime cache has no matching qualified receipt; it was preserved.",
        ),
      );
      if (JSON.stringify(cached) !== JSON.stringify(receipt))
        return blocked(
          "occupied-cache",
          "The runtime version is occupied by a different candidate; it was preserved.",
        );
      await input.validate(NodePath.join(destination, "t3"));
    } else {
      await NodeFSP.mkdir(NodePath.dirname(destination), { recursive: true, mode: 0o700 });
      const ownedScratch = await NodeFSP.mkdtemp(
        NodePath.join(NodePath.dirname(destination), ".jones-stage-"),
      );
      const scratch = NodePath.join(ownedScratch, "payload");
      try {
        await bundleFileSystem().promises.cp(payloadDirectory, scratch, {
          recursive: true,
          dereference: false,
          verbatimSymlinks: true,
          errorOnExist: true,
          force: false,
        });
        if ((await qualifiedPayloadDigest(scratch, receipt.platform)) !== digest)
          return blocked("integrity-mismatch", "Runtime payload changed during staging.");
        await input.validate(NodePath.join(scratch, "t3"));
        await durableCreate(NodePath.join(scratch, QUALIFIED_RUNTIME_RECEIPT), receipt);
        await durableCreate(NodePath.join(scratch, ".install-complete"), receipt.version);
        await NodeFSP.rename(scratch, destination);
        const directory = await NodeFSP.open(NodePath.dirname(destination), "r");
        try {
          await directory.sync();
        } finally {
          await directory.close();
        }
      } finally {
        await NodeFSP.rm(ownedScratch, { recursive: true, force: true });
      }
    }
    const staged: StagedQualifiedRuntime = {
      protocol: QUALIFIED_UPDATES_PROTOCOL,
      stagedHandle: NodeCrypto.randomUUID(),
      binding: current,
      receipt,
    };
    await durableCreate(stagedPath(current.baseDir, staged.stagedHandle), staged);
    return staged;
  });
}

export async function verifyStagedQualifiedRuntime(
  baseDir: string,
  activeVersion: string,
  handle: string,
  host?: { readonly platform: string; readonly architecture: string },
): Promise<StagedQualifiedRuntime> {
  const staged = decodeStagedQualifiedRuntime(await readRegularJson(stagedPath(baseDir, handle)));
  if (staged === undefined)
    return blocked("invalid-artifact", "Staged handle has no valid durable receipt.");
  const current = await currentQualifiedRuntimeBinding(baseDir, activeVersion, host);
  if (JSON.stringify(current) !== JSON.stringify(staged.binding))
    return blocked(
      "binding-mismatch",
      "The staged handle belongs to a different home, database, environment or active source.",
    );
  const receipt = await readQualifiedRuntimeReceipt(baseDir, staged.receipt.version, host);
  if (JSON.stringify(receipt) !== JSON.stringify(staged.receipt))
    return blocked("integrity-mismatch", "Staged receipt does not match the qualified runtime.");
  return staged;
}
