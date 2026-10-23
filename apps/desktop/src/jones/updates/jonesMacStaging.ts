// @effect-diagnostics nodeBuiltinImport:off - Native archive and detached-helper boundary.
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeModule from "node:module";
import type { JonesStagedArtifact } from "@t3tools/shared/jones/jonesActions";
import {
  createJonesStageAttempt,
  findJonesCompletedStage,
  publishJonesCompletedStage,
  validateJonesStagedArtifact,
} from "@t3tools/shared/jones/jonesActions";
import * as Schema from "effect/Schema";

import { JonesStagedMacApp, type JonesStagedMacApp as StagedMacApp } from "./jonesActivation.ts";

const AppMetadata = Schema.Struct({
  version: Schema.String,
  startupGateProtocol: Schema.optionalKey(Schema.Unknown),
  jonesSource: Schema.Struct({
    repository: Schema.Literal("Jones-Systems/Jones-Code"),
    sha: Schema.String,
    tree: Schema.String,
  }),
});
const AppInfo = Schema.Struct({
  CFBundleExecutable: Schema.String,
  CFBundleIdentifier: Schema.String,
  CFBundleShortVersionString: Schema.String,
});
const AppReceipt = Schema.Struct({
  app: JonesStagedMacApp,
  artifact: Schema.Unknown,
  candidate: Schema.Unknown,
});
const decodeAppInfo = Schema.decodeUnknownSync(AppInfo);
const decodeAppMetadata = Schema.decodeUnknownSync(AppMetadata);
const decodeAppReceipt = Schema.decodeUnknownSync(AppReceipt);

export class JonesCandidateStartupGateUnavailableError extends Error {
  readonly _tag = "JonesCandidateStartupGateUnavailableError";
  readonly requiredProtocol = 1;

  constructor() {
    super("The staged candidate does not prove startupGateProtocol:1 for its qualified source.");
  }
}

/** A marker is evidence only inside metadata bound to this staged version and source tree. */
export function requireJonesCandidateStartupGate(
  input: unknown,
  staged: Pick<StagedMacApp, "version" | "sourceSha" | "sourceTree">,
): 1 {
  let metadata: typeof AppMetadata.Type;
  try {
    metadata = decodeAppMetadata(input);
  } catch {
    throw new JonesCandidateStartupGateUnavailableError();
  }
  if (
    metadata.startupGateProtocol !== 1 ||
    metadata.version !== staged.version ||
    metadata.jonesSource.sha !== staged.sourceSha ||
    metadata.jonesSource.tree !== staged.sourceTree
  ) {
    throw new JonesCandidateStartupGateUnavailableError();
  }
  return 1;
}

export async function preflightJonesCandidateStartupGate(staged: StagedMacApp): Promise<void> {
  if (staged.startupGateProtocol !== 1) throw new JonesCandidateStartupGateUnavailableError();
  let metadata: unknown;
  try {
    metadata = JSON.parse(
      await NodeFSP.readFile(
        NodePath.join(staged.appPath, "Contents", "Resources", "app.asar", "package.json"),
        "utf8",
      ),
    );
  } catch {
    throw new JonesCandidateStartupGateUnavailableError();
  }
  requireJonesCandidateStartupGate(metadata, staged);
}

/** Publish only new task-owned paths, including parent-directory durability. */
export async function writeJonesNativeFile(
  file: string,
  content: string,
  mode: number,
): Promise<void> {
  const handle = await NodeFSP.open(file, "wx", mode);
  try {
    await handle.writeFile(content, "utf8");
    await handle.sync();
  } finally {
    await handle.close();
  }
  const directory = await NodeFSP.open(NodePath.dirname(file), "r");
  try {
    await directory.sync();
  } finally {
    await directory.close();
  }
}

export function runNativeCommand(command: string, args: readonly string[]): Promise<string> {
  return new Promise((resolve, reject) => {
    NodeChildProcess.execFile(
      command,
      [...args],
      { encoding: "utf8", maxBuffer: 1024 * 1024, timeout: 120000 },
      (error, stdout) => {
        if (error)
          reject(new Error(`Native updater operation ${NodePath.basename(command)} failed.`));
        else resolve(stdout);
      },
    );
  });
}

const nodeRequire = NodeModule.createRequire(import.meta.url);

/** Bundle integrity reads raw archives; Electron's patched filesystem presents them as directories. */
export function bundleFileSystem(
  versions: { readonly electron?: string | undefined } = process.versions,
  load: (id: string) => unknown = nodeRequire,
): typeof NodeFS {
  return versions.electron === undefined ? NodeFS : (load("original-fs") as typeof NodeFS);
}

/** Publish a completion receipt only after its copied app payload is durable. */
async function syncMacAppTree(path: string): Promise<void> {
  const fs = bundleFileSystem().promises;
  const info = await fs.lstat(path);
  if (info.isSymbolicLink()) return; // The validated internal target is synced during traversal.
  if (info.isDirectory()) {
    for (const name of await fs.readdir(path)) {
      await syncMacAppTree(NodePath.join(path, name));
    }
  } else if (!info.isFile()) {
    throw new Error("Unsupported staged app entry during synchronization.");
  }
  const handle = await fs.open(path, "r");
  try {
    await handle.sync();
  } finally {
    await handle.close();
  }
}

export async function hashMacFile(file: string): Promise<string> {
  const hash = NodeCrypto.createHash("sha256");
  for await (const block of bundleFileSystem().createReadStream(file)) hash.update(block);
  return hash.digest("hex");
}

/** Hash the complete app layout, file modes, contents, and internal symlink targets. */
export async function hashMacApp(directory: string): Promise<string> {
  const fs = bundleFileSystem().promises;
  const root = await fs.realpath(directory);
  const entries: string[] = [];
  const collect = async (current: string): Promise<void> => {
    for (const entry of await fs.readdir(current, { withFileTypes: true })) {
      const file = NodePath.join(current, entry.name);
      entries.push(file);
      if (entries.length > 200000) throw new Error("The native app exceeds its file-count bound.");
      if (entry.isDirectory()) await collect(file);
    }
  };
  await collect(root);
  entries.sort((a, b) =>
    Buffer.compare(
      Buffer.from(NodePath.relative(root, a)),
      Buffer.from(NodePath.relative(root, b)),
    ),
  );
  const hash = NodeCrypto.createHash("sha256");
  let totalBytes = 0;
  for (const file of entries) {
    const relative = NodePath.relative(root, file).split(NodePath.sep).join("/");
    const stat = await fs.lstat(file);
    let record: readonly (string | number)[];
    if (stat.isSymbolicLink()) {
      const target = await fs.realpath(file);
      const relation = NodePath.relative(root, target);
      if (relation.startsWith("..") || NodePath.isAbsolute(relation))
        throw new Error("App symlink escapes the staged app.");
      record = ["link", relative, await fs.readlink(file)];
    } else if (stat.isFile()) {
      totalBytes += stat.size;
      if (totalBytes > 8 * 1024 * 1024 * 1024)
        throw new Error("The native app exceeds its payload-size bound.");
      record = ["file", relative, stat.mode & 0o777, await hashMacFile(file)];
    } else if (stat.isDirectory()) record = ["directory", relative, stat.mode & 0o777];
    else throw new Error("Unexpected app payload file type.");
    hash.update(`${JSON.stringify(record)}\n`);
  }
  return hash.digest("hex");
}

/** Normal launches read only the bundle metadata; candidate payload hashing belongs to activation. */
export async function readMacBundleIdentity(appPath: string): Promise<{
  readonly bundleIdentifier: string;
  readonly version: string;
  readonly executablePath: string;
}> {
  const info = decodeAppInfo(
    JSON.parse(
      await runNativeCommand("/usr/bin/plutil", [
        "-convert",
        "json",
        "-o",
        "-",
        NodePath.join(appPath, "Contents", "Info.plist"),
      ]),
    ),
  );
  if (!/^[^/\\]+$/.test(info.CFBundleExecutable) || info.CFBundleIdentifier.trim() === "")
    throw new Error("The app bundle identity is invalid.");
  return {
    bundleIdentifier: info.CFBundleIdentifier,
    version: info.CFBundleShortVersionString,
    executablePath: NodePath.join(appPath, "Contents", "MacOS", info.CFBundleExecutable),
  };
}

export async function requireStableMacAppPath(appPath: string): Promise<string> {
  const resolved = await NodeFSP.realpath(appPath);
  if (
    resolved !== NodePath.resolve(appPath) ||
    !resolved.endsWith(".app") ||
    /(?:^|\/)(?:AppTranslocation|Volumes|tmp|jones-updates)(?:\/|$)/.test(resolved)
  )
    throw new Error("Move Jones Code to a stable writable Applications folder before updating.");
  const info = await NodeFSP.lstat(resolved);
  if (!info.isDirectory()) throw new Error("The running app is not a stable bundle.");
  await NodeFSP.access(NodePath.dirname(resolved), NodeFS.constants.W_OK);
  return resolved;
}

async function validateMacApp(
  directory: string,
  artifact: JonesStagedArtifact,
): Promise<{
  executablePath: string;
  asarPath: string;
  bundleIdentifier: string;
  startupGateProtocol?: 1;
}> {
  const info = decodeAppInfo(
    JSON.parse(
      await runNativeCommand("/usr/bin/plutil", [
        "-convert",
        "json",
        "-o",
        "-",
        NodePath.join(directory, "Contents", "Info.plist"),
      ]),
    ),
  );
  if (!/^[^/\\]+$/.test(info.CFBundleExecutable))
    throw new Error("Unexpected app executable path.");
  const executablePath = NodePath.join(directory, "Contents", "MacOS", info.CFBundleExecutable);
  const asarPath = NodePath.join(directory, "Contents", "Resources", "app.asar");
  // Electron's native ASAR-aware filesystem reads metadata without extracting it.
  const metadata = decodeAppMetadata(
    JSON.parse(await NodeFSP.readFile(NodePath.join(asarPath, "package.json"), "utf8")),
  );
  if (
    metadata.version !== artifact.candidate.version ||
    info.CFBundleShortVersionString !== metadata.version ||
    metadata.jonesSource.sha !== artifact.candidate.source ||
    metadata.jonesSource.tree !== artifact.candidate.tree
  )
    throw new Error("The staged app does not match the qualified source and version.");
  const architecture = await runNativeCommand("/usr/bin/lipo", ["-archs", executablePath]);
  const required = artifact.candidate.architecture === "arm64" ? "arm64" : "x86_64";
  if (!architecture.trim().split(/\s+/).includes(required))
    throw new Error("App platform or architecture does not match this host.");
  return {
    executablePath,
    asarPath,
    bundleIdentifier: info.CFBundleIdentifier,
    ...(metadata.startupGateProtocol === 1 ? { startupGateProtocol: 1 as const } : {}),
  };
}

/** An unindexed attempt may still have a mount or writer. Preserve it and stop allocating. */
async function requireNoIncompleteMacAttempts(root: string): Promise<void> {
  try {
    const rootInfo = await NodeFSP.lstat(root);
    const requireReceipt = async (directory: string, handle: string) => {
      const file = NodePath.join(directory, "mac-app-receipt.json");
      const info = await NodeFSP.lstat(file);
      if (
        !info.isFile() ||
        info.size > 131072 ||
        info.dev !== rootInfo.dev ||
        info.uid !== rootInfo.uid
      )
        throw new Error("Unknown Mac stage receipt.");
      const receipt = decodeAppReceipt(JSON.parse(await NodeFSP.readFile(file, "utf8")));
      if (
        receipt.app.handle !== handle ||
        receipt.app.receiptPath !== file ||
        NodePath.dirname(receipt.app.appPath) !== directory
      )
        throw new Error("The Mac receipt does not bind its stage.");
    };
    const entries = await NodeFSP.readdir(root);
    if (entries.length > 1000) throw new Error("Too many Mac staging records.");
    for (const name of entries) {
      const path = NodePath.join(root, name);
      const info = await NodeFSP.lstat(path);
      if (
        !info.isDirectory() ||
        info.dev !== rootInfo.dev ||
        info.uid !== rootInfo.uid ||
        (await NodeFSP.realpath(path)) !== path
      )
        throw new Error("Unknown Mac stage directory.");
      if (name === "completed") {
        const indexes = await NodeFSP.readdir(path);
        if (indexes.length > 1000) throw new Error("Too many Mac completion indexes.");
        for (const index of indexes) {
          if (!/^[a-f0-9]{64}\.json$/.test(index)) throw new Error("Unknown Mac completion index.");
          const handle = index.slice(0, -5);
          const directory = await findJonesCompletedStage(root, handle, "mac-app-receipt.json");
          if (directory === undefined) throw new Error("Missing completed Mac stage.");
          await requireReceipt(directory, handle);
        }
        continue;
      }
      if (name !== "attempts") {
        if (
          !/^[a-f0-9]{64}$/.test(name) ||
          (await findJonesCompletedStage(root, name, "mac-app-receipt.json")) !== path
        )
          throw new Error("Unknown legacy Mac stage.");
        await requireReceipt(path, name);
        continue;
      }
      const attempts = await NodeFSP.readdir(path);
      if (attempts.length > 1000) throw new Error("Too many Mac stage attempts.");
      for (const name of attempts) {
        const attempt = NodePath.join(path, name);
        const info = await NodeFSP.lstat(attempt);
        if (
          !/^stage-[a-zA-Z0-9]+$/.test(name) ||
          !info.isDirectory() ||
          info.dev !== rootInfo.dev ||
          info.uid !== rootInfo.uid ||
          (await NodeFSP.realpath(attempt)) !== attempt
        )
          throw new Error("Unknown Mac stage attempt.");
        const children = await NodeFSP.readdir(attempt);
        const handle = children.find((entry) => /^[a-f0-9]{64}$/.test(entry));
        if (
          children.length !== 2 ||
          !children.includes("completion.json") ||
          handle === undefined ||
          (await findJonesCompletedStage(root, handle, "mac-app-receipt.json")) !==
            NodePath.join(attempt, handle)
        )
          throw new Error("Unindexed Mac stage attempt.");
        await requireReceipt(NodePath.join(attempt, handle), handle);
      }
    }
  } catch (cause) {
    throw new Error(
      "An incomplete or unknown Mac stage needs exact cleanup before preparing another app.",
      { cause },
    );
  }
}

/** A command callback alone is insufficient when the owned copier's exit is unknown. */
function copyMacStage(source: string, target: string, proof: { stopped: boolean }): Promise<void> {
  proof.stopped = false;
  return new Promise((resolve, reject) => {
    let child: NodeChildProcess.ChildProcess | undefined;
    child = NodeChildProcess.execFile(
      "/usr/bin/ditto",
      [source, target],
      { encoding: "utf8", maxBuffer: 1024 * 1024, timeout: 120000 },
      (error) => {
        proof.stopped =
          child !== undefined &&
          (typeof child.exitCode === "number" || typeof child.signalCode === "string");
        if (error) reject(new Error("Native updater operation ditto failed."));
        else if (!proof.stopped)
          reject(new Error("Native updater operation ditto exit is unknown."));
        else resolve();
      },
    );
  });
}

/** Only this invocation's unselected, same-device attempt is eligible for removal. */
async function removeFailedMacAttempt(
  root: string,
  handle: string,
  attempt: { readonly directory: string; readonly attemptRoot: string },
  identity: NodeFS.Stats,
): Promise<void> {
  if ((await findJonesCompletedStage(root, handle, "mac-app-receipt.json")) === attempt.directory)
    return;
  const fs = bundleFileSystem().promises;
  const current = await fs.lstat(attempt.attemptRoot);
  if (
    !current.isDirectory() ||
    current.dev !== identity.dev ||
    current.ino !== identity.ino ||
    (await fs.realpath(attempt.attemptRoot)) !== attempt.attemptRoot
  )
    throw new Error("The failed Mac attempt's ownership changed.");
  let entries = 0;
  const inspect = async (path: string): Promise<void> => {
    if (++entries > 200_000) throw new Error("The failed Mac attempt exceeds its entry bound.");
    const info = await fs.lstat(path);
    if (info.dev !== identity.dev || info.uid !== identity.uid)
      throw new Error("The failed Mac attempt contains a mount or foreign entry.");
    if (info.isSymbolicLink() || info.isFile()) return;
    if (!info.isDirectory()) throw new Error("Unknown failed Mac attempt entry.");
    for (const name of await fs.readdir(path)) await inspect(NodePath.join(path, name));
  };
  await inspect(attempt.attemptRoot);
  await fs.rm(attempt.attemptRoot, { recursive: true });
  try {
    await fs.lstat(attempt.attemptRoot);
    throw new Error("The failed Mac attempt remains after cleanup.");
  } catch (cause) {
    if ((cause as NodeJS.ErrnoException).code !== "ENOENT") throw cause;
  }
  const parent = await fs.open(NodePath.dirname(attempt.attemptRoot), "r");
  try {
    await parent.sync();
  } finally {
    await parent.close();
  }
}

/** Download calls this only: mounting and copying never launch the candidate or use live state. */
export async function stageJonesMacApp(
  artifact: JonesStagedArtifact,
  root: string,
  platform: NodeJS.Platform,
): Promise<StagedMacApp> {
  if (platform !== "darwin" || artifact.candidate.platform !== "darwin")
    throw new Error("Mac staging requires macOS.");
  await validateJonesStagedArtifact(NodePath.dirname(artifact.payloadPath), artifact.candidate);
  const canonicalRoot = await NodeFSP.realpath(root);
  const receiptName = "mac-app-receipt.json";
  const readCompleted = async (directory: string): Promise<StagedMacApp> => {
    const receiptPath = NodePath.join(directory, receiptName);
    const receipt = decodeAppReceipt(JSON.parse(await NodeFSP.readFile(receiptPath, "utf8")));
    if (
      JSON.stringify(receipt.artifact) !== JSON.stringify(artifact) ||
      JSON.stringify(receipt.candidate) !== JSON.stringify(artifact.candidate) ||
      receipt.app.handle !== artifact.stagedHandle ||
      receipt.app.receiptPath !== receiptPath ||
      NodePath.dirname(receipt.app.appPath) !== directory
    )
      throw new Error("An occupied Mac stage does not bind this candidate; it was preserved.");
    if ((await hashMacApp(receipt.app.appPath)) !== receipt.app.appDigest)
      throw new Error("Staged app integrity changed.");
    return receipt.app;
  };
  const completed = await findJonesCompletedStage(
    canonicalRoot,
    artifact.stagedHandle,
    receiptName,
  );
  if (completed !== undefined) return readCompleted(completed);
  await requireNoIncompleteMacAttempts(canonicalRoot);
  const attempt = await createJonesStageAttempt(canonicalRoot, artifact.stagedHandle);
  const identity = await NodeFSP.lstat(attempt.attemptRoot);
  const directory = attempt.directory;
  const receiptPath = NodePath.join(directory, receiptName);
  const mount = NodePath.join(directory, "mount");
  const copier = { stopped: true };
  let detachProved = true;
  let failure: unknown;
  try {
    await NodeFSP.mkdir(mount, { mode: 0o700 });
    detachProved = false;
    await runNativeCommand("/usr/bin/hdiutil", [
      "attach",
      "-readonly",
      "-nobrowse",
      "-noautoopen",
      "-mountpoint",
      mount,
      artifact.payloadPath,
    ]);
    const apps = (await NodeFSP.readdir(mount, { withFileTypes: true })).filter(
      (entry) => entry.isDirectory() && entry.name.endsWith(".app"),
    );
    if (apps.length !== 1 || apps[0] === undefined)
      throw new Error("The DMG must contain exactly one native app.");
    const source = NodePath.join(mount, apps[0].name);
    await hashMacApp(source); // Reject escaping links before copying.
    const appPath = NodePath.join(directory, apps[0].name);
    await copyMacStage(source, appPath, copier);
    const { executablePath, asarPath, bundleIdentifier, startupGateProtocol } =
      await validateMacApp(appPath, artifact);
    const app: StagedMacApp = {
      handle: artifact.stagedHandle,
      bundleIdentifier,
      receiptPath,
      appPath,
      executablePath,
      version: artifact.candidate.version,
      sourceSha: artifact.candidate.source,
      sourceTree: artifact.candidate.tree,
      appDigest: await hashMacApp(appPath),
      asarDigest: await hashMacFile(asarPath),
      executableDigest: await hashMacFile(executablePath),
      ...(startupGateProtocol === undefined ? {} : { startupGateProtocol }),
    };
    await syncMacAppTree(appPath);
    await writeJonesNativeFile(
      receiptPath,
      JSON.stringify({ app, artifact, candidate: artifact.candidate }) + "\n",
      0o600,
    );
  } catch (cause) {
    failure = cause;
  }
  if (!detachProved) {
    try {
      // Even a failed attach can have mounted the image before losing its response.
      await runNativeCommand("/usr/bin/hdiutil", ["detach", mount]);
      await NodeFSP.rmdir(mount).catch((cause: unknown) => {
        if ((cause as NodeJS.ErrnoException).code !== "ENOENT") throw cause;
      });
      detachProved = true;
    } catch (cleanup) {
      failure =
        failure === undefined
          ? cleanup
          : new AggregateError([failure, cleanup], "Mac staging and detach failed.");
    }
  }
  if (failure === undefined) {
    try {
      const selected = await publishJonesCompletedStage(
        canonicalRoot,
        artifact.stagedHandle,
        directory,
        receiptName,
      );
      return await readCompleted(selected);
    } catch (cause) {
      failure = cause;
    }
  }
  if (!detachProved || !copier.stopped)
    throw new AggregateError(
      failure instanceof AggregateError ? failure.errors : [failure],
      "Mac staging failed; the exact attempt is held until its mount and copier exit are proved.",
    );
  try {
    await removeFailedMacAttempt(canonicalRoot, artifact.stagedHandle, attempt, identity);
  } catch (cleanup) {
    throw new AggregateError([failure, cleanup], "Mac staging and exact attempt cleanup failed.", {
      cause: cleanup,
    });
  }
  throw failure;
}
