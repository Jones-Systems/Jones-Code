/* oxlint-disable t3code/no-global-process-runtime -- Standalone launcher boundary uses native host identity; platform qualification also accepts injected test adapters. */
// @effect-diagnostics nodeBuiltinImport:off globalDate:off - Detached launcher timing stays on Node built-ins.
// Imported by the detached launcher: keep this boundary on Node built-ins.
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeModule from "node:module";
import * as NodeSqlite from "node:sqlite";
import * as NodePerfHooks from "node:perf_hooks";
import * as NodeWorkerThreads from "node:worker_threads";
import type { JonesStagedArtifact } from "@t3tools/shared/jones/jonesActions";

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

async function removeQualifiedScratch(path: string): Promise<void> {
  try {
    await bundleFileSystem().promises.rm(path, { recursive: true, force: true });
  } catch (cause) {
    // Only scratch remains here; cleanup must not replace a primary error or invalidate
    // a payload and receipt already published by atomic rename.
    console.warn("Qualified runtime scratch cleanup failed; outcome preserved.", { path, cause });
  }
}

export const QUALIFIED_UPDATES_PROTOCOL = 1 as const;
// Historical enrollment receipts need a distinct stage format that old launchers refuse.
export const RECOVERED_QUALIFIED_STAGE_PROTOCOL = 2 as const;
export const QUALIFIED_RECOVERY_PROTOCOL = 1 as const;
export const QUALIFIED_RUNTIME_RECEIPT = ".jones-runtime-receipt.json";
const HASH = /^[a-f0-9]{64}$/;
const SOURCE = /^[a-f0-9]{40}$/;
const VERSION =
  /^(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)-preview\.[0-9]+\.[0-9]+(?:\.[0-9]+)?$/;
const HANDLE = /^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/;
const localLocks = new Set<string>();

export interface QualifiedDigestTiming {
  readonly schema: 1;
  readonly role:
    | "staging"
    | "launcher-admission"
    | "launcher-activation"
    | "child-start"
    | "server-startup"
    | "selection"
    | "recovery"
    | "revocation"
    | "adoption"
    | "unspecified";
  readonly runtime: string;
  readonly files: number;
  readonly bytes: number;
  readonly wallMs: number;
  /** Process CPU includes concurrent work; wall time and loop delay distinguish contention. */
  readonly cpuMs: number;
  readonly eventLoopDelayP99Ms: number | null;
  readonly result: "match" | "mismatch" | "error";
  readonly at: string;
}

export interface QualifiedDigestOptions {
  readonly mode?: "full" | "receipt";
  readonly retainVerification?: boolean;
  /** A serving caller delegates a cache miss to its own bounded preparation worker. */
  readonly fullVerification?: (input: {
    readonly baseDir: string;
    readonly version: string;
    readonly host: { readonly platform: string; readonly architecture: string };
  }) => Promise<QualifiedRuntimeReceipt>;
  readonly signal?: AbortSignal;
  readonly role?: QualifiedDigestTiming["role"];
  readonly runtimeVersion?: string;
  /** Dedicated child processes can also declare that they have no shared serving loop. */
  readonly eventLoop?: "shared" | "worker";
  /** Used only to label the measurement; callers retain their existing integrity decisions. */
  readonly expectedSha256?: string;
  readonly onDigestTiming?: (timing: QualifiedDigestTiming) => void | Promise<void>;
}

const digestTimings: QualifiedDigestTiming[] = [];

export function recentQualifiedDigestTimings(): ReadonlyArray<QualifiedDigestTiming> {
  return digestTimings.slice();
}

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
    | ".github/workflows/artifact-desktop-mac.yml"
    | ".github/workflows/artifact-cli-mac.yml";
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
  readonly protocol: typeof QUALIFIED_UPDATES_PROTOCOL | typeof RECOVERED_QUALIFIED_STAGE_PROTOCOL;
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

export function decodeQualifiedRuntimeReceipt(value: unknown): QualifiedRuntimeReceipt | undefined {
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
      : ![
          ".github/workflows/artifact-desktop-mac.yml",
          ".github/workflows/artifact-cli-mac.yml",
        ].includes(String(r.workflow))) ||
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
    (r?.protocol !== QUALIFIED_UPDATES_PROTOCOL &&
      r?.protocol !== RECOVERED_QUALIFIED_STAGE_PROTOCOL) ||
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
  const historical = receipt.installedSourceSha !== b.activeSourceSha;
  if (historical !== (r.protocol === RECOVERED_QUALIFIED_STAGE_PROTOCOL)) return undefined;
  return {
    protocol: r.protocol,
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

/** Hash payload files and contained symlinks; auxiliary receipts never select runtime identity. */
export async function qualifiedPayloadDigest(
  directory: string,
  platform: "linux" | "darwin" = "linux",
  workflow?: QualifiedRuntimeArtifact["workflow"],
  options?: QualifiedDigestOptions,
): Promise<string> {
  if (options?.eventLoop === "shared")
    return blocked("busy", "Full runtime verification requires a preparation worker or launcher.");
  const started = NodePerfHooks.performance.now();
  const cpu = process.cpuUsage();
  const delay =
    NodeWorkerThreads.isMainThread && options?.eventLoop !== "worker"
      ? NodePerfHooks.monitorEventLoopDelay({ resolution: 10 })
      : undefined;
  delay?.enable();
  let count = 0;
  let files = 0;
  let total = 0;
  let result: QualifiedDigestTiming["result"] = "error";
  try {
    options?.signal?.throwIfAborted();
    const fs = bundleFileSystem().promises;
    const root = await fs.realpath(directory);
    const rootStat = await fs.lstat(directory);
    if (!rootStat.isDirectory() || rootStat.isSymbolicLink())
      return blocked("invalid-artifact", "Runtime payload must be a real directory.");
    const digest = NodeCrypto.createHash("sha256");
    const visit = async (relative: string): Promise<void> => {
      options?.signal?.throwIfAborted();
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
        files += 1;
        total += stat.size;
        if (total > 2 * 1024 * 1024 * 1024)
          return blocked("invalid-artifact", "Runtime payload exceeds the byte limit.");
        const hash = NodeCrypto.createHash("sha256");
        const handle = await fs.open(
          absolute,
          NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW,
        );
        try {
          for await (const bytes of handle.createReadStream()) {
            options?.signal?.throwIfAborted();
            hash.update(bytes);
          }
          options?.signal?.throwIfAborted();
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
      options?.signal?.throwIfAborted();
      if (
        name === QUALIFIED_RUNTIME_RECEIPT ||
        name === ".install-complete" ||
        name === ".jones-provenance.json"
      )
        continue;
      if (
        !(
          platform === "darwin" && workflow !== ".github/workflows/artifact-cli-mac.yml"
            ? ["t3", "Jones Code.app"]
            : ["t3", "client", "node_modules", "resource-monitor"]
        ).includes(name)
      )
        return blocked("invalid-artifact", "Runtime payload has an unexpected root entry.");
      await visit(name);
    }
    options?.signal?.throwIfAborted();
    const entry = await fs.lstat(NodePath.join(root, "t3"));
    options?.signal?.throwIfAborted();
    if (!entry.isFile() || (entry.mode & 0o111) === 0)
      return blocked("invalid-artifact", "Runtime payload has no executable.");
    const value = digest.digest("hex");
    result =
      options?.expectedSha256 === undefined || options.expectedSha256 === value
        ? "match"
        : "mismatch";
    return value;
  } finally {
    delay?.disable();
    const used = process.cpuUsage(cpu);
    const version = options?.runtimeVersion ?? NodePath.basename(directory);
    const timing: QualifiedDigestTiming = Object.freeze({
      schema: 1,
      role: options?.role ?? "unspecified",
      runtime: VERSION.test(version) ? version : "unspecified",
      files,
      bytes: total,
      wallMs: NodePerfHooks.performance.now() - started,
      cpuMs: (used.user + used.system) / 1_000,
      eventLoopDelayP99Ms:
        delay === undefined ? null : delay.count === 0 ? 0 : delay.percentile(99) / 1_000_000,
      result,
      at: new Date().toISOString(),
    });
    digestTimings.push(timing);
    if (digestTimings.length > 32) digestTimings.shift();
    try {
      const observed = options?.onDigestTiming?.(timing);
      if (observed !== undefined) void Promise.resolve(observed).catch(() => {});
    } catch {
      // Observability must never replace an integrity result or its original failure.
    }
  }
}

/** Metadata identity is a cache key only; activation still verifies all content. */
async function qualifiedPayloadFingerprint(
  directory: string,
  receipt: Pick<QualifiedRuntimeReceipt, "platform" | "workflow">,
  signal?: AbortSignal,
): Promise<string> {
  const fs = bundleFileSystem().promises;
  const root = await fs.realpath(directory);
  const rootStat = await fs.lstat(directory);
  if (!rootStat.isDirectory() || rootStat.isSymbolicLink())
    return blocked("invalid-artifact", "Runtime payload must be a real directory.");
  const digest = NodeCrypto.createHash("sha256");
  let entries = 0;
  let bytes = 0n;
  const visit = async (relative: string): Promise<void> => {
    signal?.throwIfAborted();
    const absolute = NodePath.join(root, relative);
    const stat = await fs.lstat(absolute, { bigint: true });
    if (++entries > 100_000)
      return blocked("invalid-artifact", "Runtime payload exceeds the file limit.");
    const type = stat.isSymbolicLink()
      ? "link"
      : stat.isDirectory()
        ? "dir"
        : stat.isFile()
          ? "file"
          : undefined;
    if (type === undefined)
      return blocked("invalid-artifact", "Runtime payload contains a special file.");
    digest.update(
      JSON.stringify([
        relative,
        type,
        ...[stat.size, stat.mode, stat.mtimeNs, stat.ctimeNs, stat.dev, stat.ino].map(String),
      ]) + "\n",
    );
    if (type === "link") {
      const target = await fs.readlink(absolute);
      const resolved = await fs.realpath(absolute);
      if (NodePath.isAbsolute(target) || !resolved.startsWith(`${root}${NodePath.sep}`))
        return blocked("invalid-artifact", "Runtime symlink escapes its payload.");
      digest.update(JSON.stringify(target) + "\n");
    } else if (type === "dir") {
      for (const name of (await fs.readdir(absolute)).sort())
        await visit(NodePath.join(relative, name));
    } else {
      bytes += stat.size;
      if (bytes > 2n * 1024n * 1024n * 1024n)
        return blocked("invalid-artifact", "Runtime payload exceeds the byte limit.");
    }
  };
  for (const name of (await fs.readdir(root)).sort()) {
    if ([QUALIFIED_RUNTIME_RECEIPT, ".install-complete", ".jones-provenance.json"].includes(name))
      continue;
    const allowed =
      receipt.platform === "darwin" && receipt.workflow !== ".github/workflows/artifact-cli-mac.yml"
        ? ["t3", "Jones Code.app"]
        : ["t3", "client", "node_modules", "resource-monitor"];
    if (!allowed.includes(name))
      return blocked("invalid-artifact", "Runtime payload has an unexpected root entry.");
    await visit(name);
  }
  const entry = await fs.lstat(NodePath.join(root, "t3"));
  if (!entry.isFile() || (entry.mode & 0o111) === 0)
    return blocked("invalid-artifact", "Runtime payload has no executable.");
  signal?.throwIfAborted();
  return digest.digest("hex");
}

const verificationPath = (baseDir: string, version: string) =>
  NodePath.join(baseDir, "runtime", "jones-verification", `${version}.json`);

async function checkVerificationDirectory(baseDir: string): Promise<boolean> {
  const stat = await NodeFSP.lstat(NodePath.join(baseDir, "runtime", "jones-verification")).catch(
    (cause: NodeJS.ErrnoException) => {
      if (cause.code === "ENOENT") return undefined;
      throw cause;
    },
  );
  if (stat === undefined) return false;
  if (
    !stat.isDirectory() ||
    stat.isSymbolicLink() ||
    (stat.mode & 0o022) !== 0 ||
    (typeof process.getuid === "function" && stat.uid !== process.getuid())
  )
    return blocked("invalid-artifact", "Verification cache ownership is unknown.");
  return true;
}

async function readQualifiedVerification(
  baseDir: string,
  version: string,
): Promise<Record<string, unknown> | undefined> {
  if (!(await checkVerificationDirectory(baseDir))) return undefined;
  return record(await readRegularJson(verificationPath(baseDir, version)).catch(() => undefined));
}

async function retainQualifiedVerification(
  baseDir: string,
  receipt: QualifiedRuntimeReceipt,
  fingerprint: string,
  timing: QualifiedDigestTiming | undefined,
): Promise<void> {
  const path = verificationPath(baseDir, receipt.version);
  const previous = await readQualifiedVerification(baseDir, receipt.version);
  const sameReceipt =
    JSON.stringify(decodeQualifiedRuntimeReceipt(previous?.receipt)) === JSON.stringify(receipt);
  const stagingTiming =
    timing?.role === "staging" ? timing : sameReceipt ? previous?.stagingTiming : undefined;
  await NodeFSP.mkdir(NodePath.dirname(path), { recursive: true, mode: 0o700 });
  await checkVerificationDirectory(baseDir);
  const partial = `${path}.${NodeCrypto.randomUUID()}.partial`;
  // Capture successful exclusive creation before registering removal of this exact file.
  const file = await NodeFSP.open(partial, "wx", 0o600);
  try {
    try {
      await file.writeFile(
        `${JSON.stringify({ schema: 1, receipt, fingerprint, timing, stagingTiming })}\n`,
      );
      await file.sync();
    } finally {
      await file.close();
    }
    await NodeFSP.rename(partial, path);
    const directory = await NodeFSP.open(NodePath.dirname(path), "r");
    try {
      await directory.sync();
    } finally {
      await directory.close();
    }
  } finally {
    await NodeFSP.rm(partial, { force: true });
  }
}

export async function readQualifiedRuntimeReceipt(
  baseDir: string,
  version: string,
  host: { readonly platform: string; readonly architecture: string } = {
    platform: process.platform,
    architecture: process.arch,
  },
  options?: QualifiedDigestOptions,
): Promise<QualifiedRuntimeReceipt> {
  options?.signal?.throwIfAborted();
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
  options?.signal?.throwIfAborted();
  if (receipt === undefined || receipt.version !== version)
    return blocked(
      "bootstrap-required",
      "The runtime must be enrolled with its qualified source and payload receipt.",
    );
  if (receipt.platform !== host.platform || receipt.architecture !== host.architecture)
    return blocked("invalid-artifact", "Runtime receipt does not match this host platform.");
  const directory = runtimeDirectory(baseDir, version);
  const fingerprint = await qualifiedPayloadFingerprint(directory, receipt, options?.signal);
  if (options?.mode === "receipt") {
    const cached = await readQualifiedVerification(baseDir, version);
    const cachedReceipt = decodeQualifiedRuntimeReceipt(cached?.receipt);
    if (
      cached?.schema === 1 &&
      cached.fingerprint === fingerprint &&
      JSON.stringify(cachedReceipt) === JSON.stringify(receipt)
    )
      return receipt;
    if (options.fullVerification !== undefined) {
      const verified = await options.fullVerification({ baseDir, version, host });
      if (JSON.stringify(verified) !== JSON.stringify(receipt))
        return blocked(
          "integrity-mismatch",
          "Qualified runtime changed during worker verification.",
        );
      return verified;
    }
  }
  let timing: QualifiedDigestTiming | undefined;
  if (
    (await qualifiedPayloadDigest(directory, receipt.platform, receipt.workflow, {
      ...options,
      runtimeVersion: version,
      expectedSha256: receipt.payloadSha256,
      onDigestTiming: (record) => {
        timing = record;
        return options?.onDigestTiming?.(record);
      },
    })) !== receipt.payloadSha256
  )
    return blocked("integrity-mismatch", "Qualified runtime payload changed.");
  if ((await qualifiedPayloadFingerprint(directory, receipt, options?.signal)) !== fingerprint)
    return blocked("integrity-mismatch", "Qualified runtime changed during verification.");
  if (
    options?.retainVerification === true ||
    (options?.mode === "receipt" && options.retainVerification !== false)
  )
    await retainQualifiedVerification(baseDir, receipt, fingerprint, timing);
  return receipt;
}

export async function currentQualifiedRuntimeBinding(
  baseDir: string,
  activeVersion: string,
  host?: { readonly platform: string; readonly architecture: string },
  options?: QualifiedDigestOptions,
): Promise<QualifiedRuntimeBinding> {
  options?.signal?.throwIfAborted();
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
  const receipt = await readQualifiedRuntimeReceipt(resolvedBase, activeVersion, host, options);
  return {
    baseDir: resolvedBase,
    dbPath,
    environmentId,
    activeVersion,
    activeSourceSha: receipt.sourceSha,
  };
}

export async function stageQualifiedRuntime(input: {
  readonly stagedHandle?: string;
  readonly artifact: QualifiedRuntimeArtifact;
  /** Only the preparation worker transfers its exclusively owned extraction root. Other callers copy. */
  readonly consumePayload?: boolean;
  readonly options?: QualifiedDigestOptions;
  readonly binding: QualifiedRuntimeBinding;
  readonly validate: (entryPath: string) => Promise<void>;
  readonly host?: { readonly platform: string; readonly architecture: string };
}): Promise<StagedQualifiedRuntime> {
  return withQualifiedRuntimeLock(input.binding.baseDir, "qualified-runtime-lock", async () => {
    if (input.stagedHandle !== undefined && !HANDLE.test(input.stagedHandle))
      return blocked("invalid-artifact", "Invalid fixed staged update handle.");
    const current = await currentQualifiedRuntimeBinding(
      input.binding.baseDir,
      input.binding.activeVersion,
      input.host,
      { ...input.options, mode: "receipt" },
    );
    if (JSON.stringify(current) !== JSON.stringify(input.binding))
      return blocked("binding-mismatch", "The selected native environment changed before staging.");
    const { payloadDirectory, ...candidate } = input.artifact;
    const sourceFingerprint = await qualifiedPayloadFingerprint(
      payloadDirectory,
      candidate,
      input.options?.signal,
    );
    let stagingTiming: QualifiedDigestTiming | undefined;
    const digest = await qualifiedPayloadDigest(
      payloadDirectory,
      candidate.platform,
      candidate.workflow,
      {
        ...input.options,
        runtimeVersion: candidate.version,
        role: "staging",
        onDigestTiming: (timing) => {
          stagingTiming = timing;
          return input.options?.onDigestTiming?.(timing);
        },
      },
    );
    if (
      (await qualifiedPayloadFingerprint(payloadDirectory, candidate, input.options?.signal)) !==
      sourceFingerprint
    )
      return blocked("integrity-mismatch", "Runtime payload changed during staging.");
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
    let stagedReceipt = receipt;
    const destination = runtimeDirectory(current.baseDir, receipt.version);
    const occupied = await NodeFSP.lstat(destination).then(
      () => true,
      (error: NodeJS.ErrnoException) => {
        if (error.code !== "ENOENT") throw error;
        return false;
      },
    );
    if (occupied) {
      const cached = await readQualifiedRuntimeReceipt(current.baseDir, receipt.version, host, {
        ...input.options,
        mode: "receipt",
        role: "staging",
      }).catch(() =>
        blocked(
          "occupied-cache",
          "The occupied runtime cache has no matching qualified receipt; it was preserved.",
        ),
      );
      // Enrollment history is immutable; the incoming installed source was checked
      // against the current binding above. Every artifact and payload field must match.
      const keys = new Set([...Object.keys(receipt), ...Object.keys(cached)]) as Set<
        keyof QualifiedRuntimeReceipt
      >;
      if ([...keys].some((key) => key !== "installedSourceSha" && cached[key] !== receipt[key]))
        return blocked(
          "occupied-cache",
          "The runtime version is occupied by a different candidate; it was preserved.",
        );
      await input.validate(NodePath.join(destination, "t3"));
      stagedReceipt = cached;
    } else {
      await NodeFSP.mkdir(NodePath.dirname(destination), { recursive: true, mode: 0o700 });
      const ownedScratch = await NodeFSP.mkdtemp(
        NodePath.join(NodePath.dirname(destination), ".jones-stage-"),
      );
      const scratch = NodePath.join(ownedScratch, "payload");
      try {
        if (input.consumePayload === true) {
          await NodeFSP.rename(payloadDirectory, scratch);
        } else {
          await bundleFileSystem().promises.cp(payloadDirectory, scratch, {
            recursive: true,
            dereference: false,
            verbatimSymlinks: true,
            errorOnExist: true,
            force: false,
          });
        }
        const fingerprint = await qualifiedPayloadFingerprint(
          scratch,
          receipt,
          input.options?.signal,
        );
        if (input.consumePayload === true) {
          if (fingerprint !== sourceFingerprint)
            return blocked("integrity-mismatch", "Runtime payload changed during staging.");
        } else if (
          (await qualifiedPayloadDigest(scratch, receipt.platform, receipt.workflow, {
            ...input.options,
            runtimeVersion: receipt.version,
            expectedSha256: digest,
            role: "staging",
            onDigestTiming: (timing) => {
              stagingTiming = timing;
              return input.options?.onDigestTiming?.(timing);
            },
          })) !== digest
        )
          return blocked("integrity-mismatch", "Runtime payload changed during staging.");
        await input.validate(NodePath.join(scratch, "t3"));
        if (
          (await qualifiedPayloadFingerprint(scratch, receipt, input.options?.signal)) !==
          fingerprint
        )
          return blocked("integrity-mismatch", "Runtime payload changed during staging.");
        await durableCreate(NodePath.join(scratch, QUALIFIED_RUNTIME_RECEIPT), receipt);
        await durableCreate(NodePath.join(scratch, ".install-complete"), receipt.version);
        await NodeFSP.rename(scratch, destination);
        if (
          (await qualifiedPayloadFingerprint(destination, receipt, input.options?.signal)) !==
          fingerprint
        )
          return blocked("integrity-mismatch", "Runtime payload changed during publication.");
        await retainQualifiedVerification(current.baseDir, receipt, fingerprint, stagingTiming);
        const directory = await NodeFSP.open(NodePath.dirname(destination), "r");
        try {
          await directory.sync();
        } finally {
          await directory.close();
        }
      } finally {
        await removeQualifiedScratch(ownedScratch);
      }
    }
    const staged: StagedQualifiedRuntime = {
      protocol:
        stagedReceipt.installedSourceSha === current.activeSourceSha
          ? QUALIFIED_UPDATES_PROTOCOL
          : RECOVERED_QUALIFIED_STAGE_PROTOCOL,
      stagedHandle: input.stagedHandle ?? NodeCrypto.randomUUID(),
      binding: current,
      receipt: stagedReceipt,
    };
    const path = stagedPath(current.baseDir, staged.stagedHandle);
    if (input.stagedHandle !== undefined) {
      const existing = await NodeFSP.lstat(path).catch((cause: NodeJS.ErrnoException) => {
        if (cause.code === "ENOENT") return undefined;
        throw cause;
      });
      if (existing !== undefined) {
        const prior = await verifyStagedQualifiedRuntime(
          current.baseDir,
          current.activeVersion,
          staged.stagedHandle,
          input.host,
          input.options,
        );
        if (JSON.stringify(prior) !== JSON.stringify(staged))
          return blocked("occupied-cache", "The fixed staged handle belongs to another selection.");
        // A prior publication may have succeeded before its fsync reply was lost.
        const file = await NodeFSP.open(
          path,
          NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW,
        );
        try {
          await file.sync();
        } finally {
          await file.close();
        }
        const parent = await NodeFSP.open(NodePath.dirname(path), "r");
        try {
          await parent.sync();
        } finally {
          await parent.close();
        }
        return prior;
      }
    }
    await durableCreate(path, staged);
    return staged;
  });
}

export async function verifyStagedQualifiedRuntime(
  baseDir: string,
  activeVersion: string,
  handle: string,
  host?: { readonly platform: string; readonly architecture: string },
  options?: QualifiedDigestOptions,
): Promise<StagedQualifiedRuntime> {
  options?.signal?.throwIfAborted();
  const staged = decodeStagedQualifiedRuntime(await readRegularJson(stagedPath(baseDir, handle)));
  if (staged === undefined)
    return blocked("invalid-artifact", "Staged handle has no valid durable receipt.");
  const current = await currentQualifiedRuntimeBinding(baseDir, activeVersion, host, {
    ...options,
    mode: "receipt",
  });
  if (JSON.stringify(current) !== JSON.stringify(staged.binding))
    return blocked(
      "binding-mismatch",
      "The staged handle belongs to a different home, database, environment or active source.",
    );
  const receipt = await readQualifiedRuntimeReceipt(baseDir, staged.receipt.version, host, options);
  if (JSON.stringify(receipt) !== JSON.stringify(staged.receipt))
    return blocked("integrity-mismatch", "Staged receipt does not match the qualified runtime.");
  return staged;
}

/** Enrolls an independently verified bootstrap payload; never replaces an occupied runtime. */
export async function enrollQualifiedRuntime(input: {
  readonly artifact: QualifiedRuntimeArtifact;
  readonly baseDir: string;
  readonly host: { readonly platform: string; readonly architecture: string };
  readonly validate: (entryPath: string) => Promise<void>;
  readonly prepareExistingRuntime?: (directory: string) => Promise<void>;
}): Promise<QualifiedRuntimeReceipt> {
  const { payloadDirectory, ...artifact } = input.artifact;
  const digest = await qualifiedPayloadDigest(
    payloadDirectory,
    artifact.platform,
    artifact.workflow,
    { runtimeVersion: artifact.version },
  );
  const receipt = decodeQualifiedRuntimeReceipt({
    ...artifact,
    protocol: 1,
    payloadSha256: digest,
  });
  if (
    receipt === undefined ||
    receipt.platform !== input.host.platform ||
    receipt.architecture !== input.host.architecture
  )
    return blocked("invalid-artifact", "Bootstrap artifact does not match this host.");
  const destination = runtimeDirectory(input.baseDir, receipt.version);
  const existing = await NodeFSP.lstat(destination).catch((error: NodeJS.ErrnoException) => {
    if (error.code !== "ENOENT") throw error;
    return undefined;
  });
  if (existing !== undefined) {
    if (
      !existing.isDirectory() ||
      existing.isSymbolicLink() ||
      (await qualifiedPayloadDigest(destination, receipt.platform, receipt.workflow, {
        runtimeVersion: receipt.version,
        expectedSha256: digest,
      })) !== digest
    )
      return blocked(
        "occupied-cache",
        "Existing runtime differs from the verified artifact; it was preserved.",
      );
    const current = await NodeFSP.lstat(
      NodePath.join(destination, QUALIFIED_RUNTIME_RECEIPT),
    ).catch((error: NodeJS.ErrnoException) => {
      if (error.code !== "ENOENT") throw error;
      return undefined;
    });
    if (current !== undefined) {
      const observed = await readQualifiedRuntimeReceipt(
        input.baseDir,
        receipt.version,
        input.host,
      );
      const keys = Object.keys(receipt) as Array<keyof QualifiedRuntimeReceipt>;
      if (keys.some((key) => key !== "installedSourceSha" && observed[key] !== receipt[key]))
        return blocked("occupied-cache", "Existing qualified receipt differs; it was preserved.");
      await input.validate(NodePath.join(destination, "t3"));
      return observed;
    }
    const marker = await NodeFSP.readFile(NodePath.join(destination, ".install-complete"), "utf8");
    if (marker.trim() !== receipt.version)
      return blocked("occupied-cache", "Existing runtime is incomplete; it was preserved.");
    if (input.prepareExistingRuntime !== undefined) {
      await input.prepareExistingRuntime(destination);
      if (
        (await qualifiedPayloadDigest(destination, receipt.platform, receipt.workflow, {
          runtimeVersion: receipt.version,
          expectedSha256: digest,
        })) !== digest
      )
        return blocked(
          "integrity-mismatch",
          "Existing runtime changed during bootstrap preparation.",
        );
    }
    await input.validate(NodePath.join(destination, "t3"));
    await durableCreate(NodePath.join(destination, QUALIFIED_RUNTIME_RECEIPT), receipt);
    return receipt;
  }
  await NodeFSP.mkdir(NodePath.dirname(destination), { recursive: true, mode: 0o700 });
  const owner = await NodeFSP.mkdtemp(
    NodePath.join(NodePath.dirname(destination), ".jones-adopt-"),
  );
  const scratch = NodePath.join(owner, "payload");
  try {
    await bundleFileSystem().promises.cp(payloadDirectory, scratch, {
      recursive: true,
      dereference: false,
      verbatimSymlinks: true,
      errorOnExist: true,
      force: false,
    });
    if (
      (await qualifiedPayloadDigest(scratch, receipt.platform, receipt.workflow, {
        runtimeVersion: receipt.version,
        expectedSha256: digest,
      })) !== digest
    )
      return blocked("integrity-mismatch", "Bootstrap payload changed while copied.");
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
    await removeQualifiedScratch(owner);
  }
  return receipt;
}

/** Staging evidence is read before activation refreshes the verification cache. */
export async function qualifiedStagingTiming(
  baseDir: string,
  version: string,
): Promise<QualifiedDigestTiming | undefined> {
  if (!VERSION.test(version)) return undefined;
  const cached = await readQualifiedVerification(baseDir, version);
  const value = record(cached?.stagingTiming);
  if (
    value?.schema !== 1 ||
    value.role !== "staging" ||
    value.runtime !== version ||
    !["match", "mismatch", "error"].includes(String(value.result)) ||
    typeof value.at !== "string" ||
    !Number.isFinite(Date.parse(value.at)) ||
    ["files", "bytes", "wallMs", "cpuMs"].some(
      (key) =>
        typeof value[key] !== "number" || !Number.isFinite(value[key]) || Number(value[key]) < 0,
    ) ||
    (value.eventLoopDelayP99Ms !== null &&
      (typeof value.eventLoopDelayP99Ms !== "number" ||
        !Number.isFinite(value.eventLoopDelayP99Ms) ||
        value.eventLoopDelayP99Ms < 0))
  )
    return undefined;
  return value as unknown as QualifiedDigestTiming;
}

/** Diagnostics have their own exclusive files and never alter operation-ledger receipt bytes. */
export async function retainQualifiedAttemptTimings(
  baseDir: string,
  operationId: string,
  timings: ReadonlyArray<QualifiedDigestTiming>,
): Promise<void> {
  if (!HANDLE.test(operationId) || timings.length > 128)
    throw new Error("Invalid bounded update timing record.");
  const directory = NodePath.join(baseDir, "runtime", "jones-update-timing");
  await NodeFSP.mkdir(directory, { recursive: true, mode: 0o700 });
  const owned = await NodeFSP.lstat(directory);
  if (
    !owned.isDirectory() ||
    owned.isSymbolicLink() ||
    (owned.mode & 0o077) !== 0 ||
    (typeof process.getuid === "function" && owned.uid !== process.getuid())
  )
    throw new Error("Update timing directory ownership is unknown.");
  const path = NodePath.join(directory, `${operationId}.json`);
  await durableCreate(path, { schema: 1, operationId, records: timings }).catch(
    (cause: NodeJS.ErrnoException) => {
      if (cause.code !== "EEXIST") throw cause;
    },
  );
  const retained: { path: string; mtime: number }[] = [];
  const names = await NodeFSP.readdir(directory);
  if (names.length > 4096)
    throw new Error("Update timing retention inspection exceeded its bound.");
  for (const name of names) {
    if (!name.endsWith(".json") || !HANDLE.test(name.slice(0, -5))) continue;
    const file = NodePath.join(directory, name);
    const stat = await NodeFSP.lstat(file);
    if (
      !stat.isFile() ||
      stat.isSymbolicLink() ||
      (stat.mode & 0o077) !== 0 ||
      stat.size > 64 * 1024 ||
      (typeof process.getuid === "function" && stat.uid !== process.getuid())
    )
      continue;
    retained.push({ path: file, mtime: stat.mtimeMs });
  }
  retained.sort((a, b) => b.mtime - a.mtime || a.path.localeCompare(b.path));
  for (const older of retained.slice(64)) await NodeFSP.unlink(older.path);
  const parent = await NodeFSP.open(directory, "r");
  try {
    await parent.sync();
  } finally {
    await parent.close();
  }
}
