// @effect-diagnostics nodeBuiltinImport:off preferSchemaOverJson:off -- Same-build process bootstrap only; no provider launcher or inherited loader hooks.
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeSea from "node:sea";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { COMMAND, buildId, type Bootstrap } from "./protocol.ts";
import { computeFailure } from "./failure.ts";

// Root's Vite hook stamps every chunk with the same exact source-candidate digest.
// Hashing only bin.js would not bind a code-split worker chunk in bundled Node/Electron.
declare const __JONES_SERVER_COMPUTE_BUILD_ID__: string | undefined;

const sourceFiles = [
  "jones/serverCompute/worker.ts",
  "jones/serverCompute/guardian.ts",
  "jones/serverCompute/protocol.ts",
  "jones/serverCompute/failure.ts",
  "jones/serverCompute/workspaceErrors.ts",
  "jones/serverCompute/workerCommand.ts",
  "jones/serverCompute/ServerCompute.ts",
  "jones/serverCompute/journal.ts",
  "jones/serverCompute/processSlot.ts",
  "jones/serverCompute/supervisor.ts",
  "jones/serverParallelism/computeContract.ts",
  "jones/serverParallelism/config.ts",
  "jones/serverParallelism/observations.ts",
  "jones/workspace/nativeSearchRuntime.ts",
  "jones/workspace/contentSearchPages.ts",
];
export interface WorkerCommand {
  readonly file: string;
  readonly args: readonly string[];
  readonly env: NodeJS.ProcessEnv;
  readonly build: string;
}
/** The worker independently hashes its own same-build entry before any native load. */
export async function currentBuildIdentity(): Promise<string> {
  const sea = NodeSea.isSea();
  if (!sea && typeof __JONES_SERVER_COMPUTE_BUILD_ID__ !== "undefined") {
    if (!buildId(__JONES_SERVER_COMPUTE_BUILD_ID__)) throw computeFailure("unavailable");
    return __JONES_SERVER_COMPUTE_BUILD_ID__;
  }
  const entry = sea ? process.execPath : process.argv[1];
  if (!entry || !NodePath.isAbsolute(entry) || (!sea && !entry.endsWith(".ts")))
    throw computeFailure("unavailable");
  const hash = NodeCrypto.createHash("sha256");
  const files = entry.endsWith(".ts")
    ? [entry, ...sourceFiles.map((path) => NodePath.join(NodePath.dirname(entry), path))]
    : [entry];
  for (const file of files) {
    const fileHash = NodeCrypto.createHash("sha256");
    for await (const data of NodeFS.createReadStream(file)) fileHash.update(data);
    hash.update(
      JSON.stringify({
        path: file === entry ? "entry" : file.slice(NodePath.dirname(entry).length),
        sha256: fileHash.digest("hex"),
      }) + "\n",
    );
  }
  return hash.digest("hex");
}
export function minimalWorkerEnvironment(
  source: NodeJS.ProcessEnv,
  sea: boolean,
  electron: boolean,
): NodeJS.ProcessEnv {
  const result: NodeJS.ProcessEnv = {};
  for (const key of [
    "PATH",
    "HOME",
    "TMPDIR",
    "TMP",
    "TEMP",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "SystemRoot",
    "WINDIR",
  ]) {
    const value = source[key];
    if (value !== undefined) result[key] = value;
  }
  if (electron) result.ELECTRON_RUN_AS_NODE = "1";
  // SEA treats argv flags as application arguments; NODE_OPTIONS is deliberately replaced, never inherited.
  if (sea) result.NODE_OPTIONS = "--max-old-space-size=256";
  return result;
}
export async function resolveWorkerCommand(
  platform: NodeJS.Platform = HostProcess.Platform.defaultValue(),
): Promise<WorkerCommand> {
  if (platform !== "darwin" && platform !== "linux")
    throw computeFailure("unavailable");
  const sea = NodeSea.isSea(),
    electron = process.versions.electron !== undefined;
  const entry = process.argv[1];
  if (!sea && (!entry || !NodePath.isAbsolute(entry))) throw computeFailure("unavailable");
  if (!sea) await NodeFSP.access(entry!);
  // Built-in TypeScript stripping in the supported Node release needs no inherited loader, inspector or broad NODE_OPTIONS.
  if (!sea && entry!.endsWith(".ts") && Number(process.versions.node.split(".")[0]) < 26)
    throw computeFailure("unavailable");
  return {
    file: process.execPath,
    args: sea ? [COMMAND] : ["--max-old-space-size=256", entry!, COMMAND],
    env: minimalWorkerEnvironment(process.env, sea, electron),
    build: await currentBuildIdentity(),
  };
}
export function bootstrapArgument(bootstrap: Bootstrap): string {
  return JSON.stringify(bootstrap);
}
