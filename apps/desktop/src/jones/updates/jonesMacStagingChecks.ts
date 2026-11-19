// @effect-diagnostics nodeBuiltinImport:off - Shared metadata/path checks and ordinary-UID native file/command ports; no payload filesystem loader.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as Schema from "effect/Schema";
import type { JonesStagedMacApp as StagedMacApp } from "./jonesActivation.ts";

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
export const decodeAppInfo = Schema.decodeUnknownSync(AppInfo);
export const decodeAppMetadata = Schema.decodeUnknownSync(AppMetadata);
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

export function runNativeCommand(
  command: string,
  args: readonly string[],
  timeout = 120000,
): Promise<string> {
  return new Promise((resolve, reject) => {
    NodeChildProcess.execFile(
      command,
      [...args],
      { encoding: "utf8", maxBuffer: 1024 * 1024, timeout },
      (error, stdout) => {
        if (error)
          reject(new Error(`Native updater operation ${NodePath.basename(command)} failed.`));
        else resolve(stdout);
      },
    );
  });
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
