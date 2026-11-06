// @effect-diagnostics nodeBuiltinImport:off -- Metadata-only observation of the exact PTY owned by this Manager; no process scan or contents.
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeTimers from "node:timers";
import * as Context from "effect/Context";
import { nativePlatform } from "./Filesystem.ts";
import type { DutyIdentity } from "./DutyIdentity.ts";

async function observe(pid: number, physical: DutyIdentity | undefined): Promise<string | null> {
  if (!Number.isSafeInteger(pid) || pid <= 0 || physical?.status !== "bound") return null;
  const read = async () => {
    let cwd: string;
    if (nativePlatform === "linux") cwd = await NodeFSP.readlink(`/proc/${pid}/cwd`);
    else if (nativePlatform === "darwin") {
      const output = await new Promise<string>((resolve, reject) => NodeChildProcess.execFile(
        "/usr/sbin/lsof", ["-a", "-p", String(pid), "-d", "cwd", "-Fn"],
        {timeout: 80, maxBuffer: 8192, encoding: "utf8"},
        (error, result) => error ? reject(error) : resolve(result),
      ));
      const paths = output.split("\n").filter(line => line.startsWith("n")).map(line => line.slice(1));
      if (paths.length !== 1) return null;
      cwd = paths[0]!;
    } else return null;
    if (!NodePath.isAbsolute(cwd) || cwd.includes("\0")) return null;
    const root = physical.binding.path;
    const [stat, real] = await Promise.all([NodeFSP.lstat(root, {bigint: true}), NodeFSP.realpath(cwd)]);
    if (!stat.isDirectory() || stat.isSymbolicLink() || String(stat.dev) !== physical.binding.dev || String(stat.ino) !== physical.binding.ino) return null;
    return real === root || real.startsWith(root + NodePath.sep) ? real : null;
  };
  let timer: ReturnType<typeof NodeTimers.setTimeout> | undefined;
  try {
    return await Promise.race([read(), new Promise<null>(resolve => {timer = NodeTimers.setTimeout(() => resolve(null), 100);})]);
  } catch {return null;}
  finally {if (timer) NodeTimers.clearTimeout(timer);}
}
export class TerminalCwdObservation extends Context.Reference<{
  readonly observe: typeof observe;
}>("jones/workspaceRetirement/TerminalCwdObservation", {defaultValue: () => ({observe})}) {}
