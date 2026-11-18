// @effect-diagnostics nodeBuiltinImport:off -- Fixed ordinary-user metadata helper, with inherited read-only proc anchor and bounded lifetime.
// @effect-diagnostics globalTimers:off -- Promise adapter enforces and reaps its owned native process group within the collector deadline.
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeChildProcess from "node:child_process";
import { linuxInfrastructureHelper } from "./LinuxInfrastructureHelper.ts";

export const INFRASTRUCTURE_POLICY = "linux-systemd-user-manager-main/v1" as const;
const infrastructureReasons = [
  "unavailable",
  "identity",
  "provenance",
  "changed",
  "bounds",
  "timeout",
] as const;
export type InfrastructureReason = (typeof infrastructureReasons)[number];
export type InfrastructureResult =
  | { kind: "verified"; basisDigest: string }
  | { kind: "unclassified"; reason: InfrastructureReason };
export interface LinuxProcessAnchor {
  readonly base: string;
  readonly fd: number;
  readonly snapshot: string;
  close(): Promise<void>;
}
const digest = (value: string) => NodeCrypto.createHash("sha256").update(value).digest("hex");
export function infrastructureBasis(digests: readonly string[]) {
  return digest(JSON.stringify([INFRASTRUCTURE_POLICY, [...digests].sort()]));
}
async function bounded(path: string, limit: number) {
  const file = await NodeFSP.open(path, "r");
  try {
    const bytes = Buffer.alloc(limit + 1);
    const { bytesRead } = await file.read(bytes, 0, bytes.length, 0);
    if (bytesRead > limit) throw Error("bound_exceeded");
    return bytes.subarray(0, bytesRead).toString();
  } finally {
    await file.close();
  }
}
export async function captureLinuxProcess(pid: string): Promise<LinuxProcessAnchor> {
  if (!/^[1-9]\d{0,9}$/.test(pid)) throw Error("invalid_pid");
  const file = await NodeFSP.open("/proc/" + pid, "r");
  try {
    const base = "/proc/self/fd/" + file.fd;
    const status = await bounded(base + "/status", 65536);
    const stat = await bounded(base + "/stat", 4096);
    const cgroup = await bounded(base + "/cgroup", 65536);
    const identity = await file.stat({ bigint: true });
    const field = (name: string) =>
      status
        .match(new RegExp("^" + name + ":\\s*(.*)$", "m"))?.[1]
        ?.trim()
        .split(/\s+/) ?? [];
    const tail = stat
      .slice(stat.lastIndexOf(")") + 2)
      .trim()
      .split(/\s+/);
    const uids = field("Uid"),
      nspid = field("NSpid");
    if (
      field("Pid")[0] !== pid ||
      uids.length !== 4 ||
      !uids.every((x) => /^\d+$/.test(x)) ||
      !tail[19] ||
      !tail[1]
    )
      throw Error("invalid_proc_metadata");
    const snapshot = digest(
      JSON.stringify([
        pid,
        tail[19],
        tail[1],
        uids,
        nspid,
        cgroup,
        String(identity.dev),
        String(identity.ino),
      ]),
    );
    return { base, fd: file.fd, snapshot, close: () => file.close() };
  } catch (cause) {
    await file.close();
    throw cause;
  }
}
async function trustedPython() {
  // Validate the interpreter before executing any supplied code, including every resolved component.
  let pending = ["usr", "bin", "python3"],
    resolved = "/",
    steps = 0;
  for (;;) {
    const root = await NodeFSP.lstat(resolved);
    if (root.uid !== 0 || (root.mode & 0o022) !== 0 || !root.isDirectory()) return false;
    const component = pending.shift();
    if (!component) return false;
    if (++steps > 32 || component === ".." || component === "." || !component) return false;
    const path = resolved === "/" ? "/" + component : resolved + "/" + component;
    const meta = await NodeFSP.lstat(path);
    if (meta.uid !== 0) return false;
    if (meta.isSymbolicLink()) {
      const link = await NodeFSP.readlink(path);
      if (!link.startsWith("/")) pending.unshift(...link.split("/"));
      else {
        resolved = "/";
        pending.unshift(...link.slice(1).split("/"));
      }
      continue;
    }
    if ((meta.mode & 0o022) !== 0) return false;
    if (!pending.length) return meta.isFile();
    if (!meta.isDirectory()) return false;
    resolved = path;
  }
}
export async function verifyLinuxInfrastructure(
  anchor: LinuxProcessAnchor,
  pid: string,
  uid: number,
  remainingMs: number,
): Promise<InfrastructureResult> {
  const started = performance.now();
  try {
    if (
      !Number.isSafeInteger(uid) ||
      uid < 0 ||
      !/^[1-9]\d{0,9}$/.test(pid) ||
      !Number.isFinite(remainingMs)
    )
      return { kind: "unclassified", reason: "identity" };
    if (!(await trustedPython())) return { kind: "unclassified", reason: "provenance" };
    const timeout = Math.floor(Math.min(1500, remainingMs - (performance.now() - started)));
    if (timeout < 1) return { kind: "unclassified", reason: "timeout" };
    return await new Promise<InfrastructureResult>((resolve) => {
      const child = NodeChildProcess.spawn(
        "/usr/bin/python3",
        [
          "-I",
          "-B",
          "-c",
          linuxInfrastructureHelper,
          pid,
          String(uid),
          anchor.snapshot,
          String(timeout),
        ],
        {
          stdio: ["ignore", "pipe", "ignore", anchor.fd],
          detached: true,
          env: { PATH: "/usr/bin:/bin", LC_ALL: "C" },
        },
      );
      const stop = () => {
        if (child.pid) {
          try {
            process.kill(-child.pid, "SIGKILL");
          } catch {
            /* Already exited. */
          }
        }
      };
      let output = "",
        overflow = false,
        timedOut = false;
      const timer = setTimeout(() => {
        timedOut = true;
        stop();
      }, timeout);
      child.stdout!.on("data", (chunk: Buffer) => {
        if (output.length + chunk.length > 65536) {
          overflow = true;
          stop();
        } else output += chunk.toString();
      });
      child.on("error", () => {
        clearTimeout(timer);
        resolve({ kind: "unclassified", reason: "unavailable" });
      });
      child.on("close", (code) => {
        clearTimeout(timer);
        if (timedOut || overflow || code !== 0)
          return resolve({
            kind: "unclassified",
            reason: timedOut ? "timeout" : overflow ? "bounds" : "unavailable",
          });
        try {
          const value = JSON.parse(output) as InfrastructureResult;
          if (value.kind === "verified" && /^[a-f0-9]{64}$/.test(value.basisDigest))
            return resolve(value);
          if (value.kind === "unclassified" && infrastructureReasons.includes(value.reason))
            return resolve(value);
        } catch {
          /* All helper failures remain sanitized uncertainty. */
        }
        resolve({ kind: "unclassified", reason: "unavailable" });
      });
    });
  } catch {
    return { kind: "unclassified", reason: "unavailable" };
  }
}
