// @effect-diagnostics nodeBuiltinImport:off - Native archive and detached-helper boundary.
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeModule from "node:module";
import type { JonesStagedArtifact } from "@t3tools/shared/jones/jonesActions";
import { validateJonesStagedArtifact } from "@t3tools/shared/jones/jonesActions";
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

async function validateMacApp(
  directory: string,
  artifact: JonesStagedArtifact,
): Promise<{ executablePath: string; asarPath: string; startupGateProtocol?: 1 }> {
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
    ...(metadata.startupGateProtocol === 1 ? { startupGateProtocol: 1 as const } : {}),
  };
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
  const directory = NodePath.join(await NodeFSP.realpath(root), artifact.stagedHandle);
  const receiptPath = NodePath.join(directory, "mac-app-receipt.json");
  try {
    await NodeFSP.mkdir(directory, { mode: 0o700 });
  } catch (cause) {
    if ((cause as NodeJS.ErrnoException).code !== "EEXIST") throw cause;
    const receipt = decodeAppReceipt(JSON.parse(await NodeFSP.readFile(receiptPath, "utf8")));
    if (
      JSON.stringify(receipt.artifact) !== JSON.stringify(artifact) ||
      receipt.app.handle !== artifact.stagedHandle
    ) {
      throw new Error("An occupied Mac stage does not bind this candidate; it was preserved.");
    }
    if ((await hashMacApp(receipt.app.appPath)) !== receipt.app.appDigest)
      throw new Error("Staged app integrity changed.");
    return receipt.app;
  }
  const mount = NodePath.join(directory, "mount");
  await NodeFSP.mkdir(mount, { mode: 0o700 });
  let mounted = false;
  try {
    await runNativeCommand("/usr/bin/hdiutil", [
      "attach",
      "-readonly",
      "-nobrowse",
      "-noautoopen",
      "-mountpoint",
      mount,
      artifact.payloadPath,
    ]);
    mounted = true;
    const apps = (await NodeFSP.readdir(mount, { withFileTypes: true })).filter(
      (entry) => entry.isDirectory() && entry.name.endsWith(".app"),
    );
    if (apps.length !== 1 || apps[0] === undefined)
      throw new Error("The DMG must contain exactly one native app.");
    const source = NodePath.join(mount, apps[0].name);
    await hashMacApp(source); // Reject escaping links before copying.
    const appPath = NodePath.join(directory, apps[0].name);
    await runNativeCommand("/usr/bin/ditto", [source, appPath]);
    const { executablePath, asarPath, startupGateProtocol } = await validateMacApp(appPath, artifact);
    const app: StagedMacApp = {
      handle: artifact.stagedHandle,
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
    await writeJonesNativeFile(
      receiptPath,
      JSON.stringify({ app, artifact, candidate: artifact.candidate }) + "\n",
      0o600,
    );
    return app;
  } finally {
    if (mounted) await runNativeCommand("/usr/bin/hdiutil", ["detach", mount]);
    // Only the newly-created empty mount directory is cleanup-owned. Failed app stages remain for inspection.
    await NodeFSP.rmdir(mount).catch(() => undefined);
  }
}
