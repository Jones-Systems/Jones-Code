// @effect-diagnostics nodeBuiltinImport:off
// Manager membership is inspected only at a fenced handoff, never on ordinary startup.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

interface ProcessSnapshot {
  readonly text: string;
  /** The captured inventory subprocess has exited before this snapshot is returned. */
  readonly exitedInventoryPid?: number;
}

export interface LauncherHandoffProcessIO {
  readonly platform: string;
  readonly uid: number;
  readonly readText: (file: string) => Promise<string>;
  readonly previousService: (program: string) => Promise<string>;
  readonly managerSnapshot: (service: string) => Promise<string>;
  readonly darwinSnapshot: () => Promise<ProcessSnapshot>;
}

async function readKernelText(file: string): Promise<string> {
  const descriptor = await NodeFSP.open(
    file,
    NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW,
  );
  try {
    const buffer = Buffer.alloc(65537);
    let total = 0;
    for (;;) {
      const { bytesRead } = await descriptor.read(buffer, total, buffer.length - total, null);
      total += bytesRead;
      if (total > 65536) throw new Error("Manager membership exceeds its bounded inventory.");
      if (bytesRead === 0) return buffer.subarray(0, total).toString("utf8");
    }
  } finally {
    await descriptor.close();
  }
}

const nativeIO: LauncherHandoffProcessIO = {
  // oxlint-disable-next-line t3code/no-global-process-runtime -- The detached launcher uses Node built-ins; tests inject this native boundary.
  platform: NodeOS.platform(),
  uid: NodeOS.userInfo().uid,
  readText: readKernelText,
  previousService: async (program) => {
    const directory = NodePath.dirname(program);
    const stat = await NodeFSP.lstat(directory);
    const file = NodePath.join(directory, "previous-service-definition");
    const saved = await NodeFSP.lstat(file);
    if (
      !stat.isDirectory() ||
      stat.uid !== NodeOS.userInfo().uid ||
      (stat.mode & 0o777) !== 0o700 ||
      (await NodeFSP.realpath(directory)) !== directory ||
      !saved.isFile() ||
      saved.uid !== NodeOS.userInfo().uid ||
      (saved.mode & 0o777) !== 0o600 ||
      saved.size > 65536
    )
      throw new Error("Launcher manager preimage has unknown ownership.");
    return readKernelText(file);
  },
  managerSnapshot: (service) =>
    new Promise((resolve, reject) => {
      const linux = nativeIO.platform === "linux";
      NodeChildProcess.execFile(
        linux ? "/usr/bin/systemctl" : "/bin/launchctl",
        linux
          ? [
              "--user",
              "show",
              service,
              "--property=MainPID",
              "--property=ControlGroup",
              "--property=ExecStart",
            ]
          : ["print", "gui/" + NodeOS.userInfo().uid + "/" + service],
        { timeout: 5000, maxBuffer: 16384, encoding: "utf8" },
        (error, text) => (error === null ? resolve(text) : reject(error)),
      );
    }),
  darwinSnapshot: () =>
    new Promise((resolve, reject) => {
      const child = NodeChildProcess.execFile(
        "/bin/ps",
        ["-axo", "pid=,ppid=,pgid=,uid="],
        { timeout: 5000, maxBuffer: 1024 * 1024, encoding: "utf8" },
        (error, text) =>
          error === null
            ? resolve({
                text,
                ...(child.pid === undefined ? {} : { exitedInventoryPid: child.pid }),
              })
            : reject(error),
      );
    }),
};

function unifiedGroup(contents: string): string | undefined {
  const groups = contents.split("\n").filter((line) => line.startsWith("0::"));
  if (groups.length !== 1) return undefined;
  const path = groups[0]?.slice(3);
  return path !== undefined &&
    path.startsWith("/") &&
    path !== "/" &&
    !/[\u0000-\u0020\u007f]/.test(path) &&
    path
      .split("/")
      .slice(1)
      .every((part) => part !== "" && part !== "." && part !== "..")
    ? path
    : undefined;
}

/** Unknown, escaped or additional manager members defer; no process is signalled here. */
export async function launcherHandoffProcessesQuiet(
  launcherPid: number,
  childPid: number,
  expectedProgram: string,
  io: LauncherHandoffProcessIO = nativeIO,
): Promise<boolean> {
  if (
    !Number.isSafeInteger(launcherPid) ||
    !Number.isSafeInteger(childPid) ||
    launcherPid <= 0 ||
    childPid <= 0 ||
    launcherPid === childPid
  )
    return false;
  try {
    if (io.platform === "linux") {
      const group = unifiedGroup(await io.readText("/proc/" + launcherPid + "/cgroup"));
      if (
        group === undefined ||
        unifiedGroup(await io.readText("/proc/" + childPid + "/cgroup")) !== group
      )
        return false;
      const service = group.split("/").at(-1);
      if (service === undefined || !/^[A-Za-z0-9_.@:\\-]+\.service$/.test(service)) return false;
      const manager = await io.managerSnapshot(service);
      const managerPid = /^MainPID=([1-9]\d*)$/m.exec(manager)?.[1];
      const controlGroup = /^ControlGroup=(.+)$/m.exec(manager)?.[1];
      const program = /^ExecStart=\{\s*path=(.*?)\s*;/m.exec(manager)?.[1];
      if (
        Number(managerPid) !== launcherPid ||
        controlGroup !== group ||
        program !== expectedProgram
      )
        return false;
      // cgroup.procs excludes nested groups; refuse them rather than overlooking descendants.
      const statistics = await io.readText("/sys/fs/cgroup" + group + "/cgroup.stat");
      if (!/^nr_descendants 0$/m.test(statistics) || !/^nr_dying_descendants 0$/m.test(statistics))
        return false;
      const contents = await io.readText("/sys/fs/cgroup" + group + "/cgroup.procs");
      const lines = contents.trim().split("\n");
      if (lines.length > 4096 || lines.some((line) => !/^[1-9]\d*$/.test(line))) return false;
      const members = new Set(lines.map(Number));
      return members.size === 2 && members.has(launcherPid) && members.has(childPid);
    }
    if (io.platform === "darwin") {
      const preimage: unknown = JSON.parse(await io.previousService(expectedProgram));
      if (
        typeof preimage !== "object" ||
        preimage === null ||
        !("unitContents" in preimage) ||
        typeof preimage.unitContents !== "string"
      )
        return false;
      const labels = [
        ...preimage.unitContents.matchAll(
          /<key>Label<\/key>\s*<string>([A-Za-z0-9_.-]+)<\/string>/g,
        ),
      ];
      if (labels.length !== 1 || labels[0]?.[1] === undefined) return false;
      const manager = await io.managerSnapshot(labels[0][1]);
      if (
        Number(/^\s*pid = ([1-9]\d*)\s*$/m.exec(manager)?.[1]) !== launcherPid ||
        /^\s*program = (.+?)\s*$/m.exec(manager)?.[1] !== expectedProgram
      )
        return false;
      const snapshot = await io.darwinSnapshot();
      const lines = snapshot.text.trim().split("\n");
      if (lines.length > 32768) return false;
      const rows = lines.map((line) => /^\s*(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s*$/.exec(line));
      if (rows.some((row) => row === null)) return false;
      const processes = rows.map((row) => ({
        pid: Number(row?.[1]),
        parent: Number(row?.[2]),
        group: Number(row?.[3]),
        uid: Number(row?.[4]),
      }));
      if (
        processes.some(
          (row) =>
            ![row.pid, row.parent, row.group, row.uid].every(
              (value) => Number.isSafeInteger(value) && value >= 0,
            ),
        ) ||
        new Set(processes.map((row) => row.pid)).size !== processes.length
      )
        return false;
      const permitted = (row: (typeof processes)[number]) =>
        row.pid === launcherPid ||
        row.pid === childPid ||
        (row.pid === snapshot.exitedInventoryPid &&
          row.parent === launcherPid &&
          row.uid === io.uid);
      const children = new Map<number, Array<(typeof processes)[number]>>();
      for (const row of processes) {
        if (row.pid === row.parent) continue;
        const siblings = children.get(row.parent) ?? [];
        siblings.push(row);
        children.set(row.parent, siblings);
      }
      const queue = [launcherPid];
      const seen = new Set<number>();
      while (queue.length > 0) {
        const parent = queue.pop();
        if (parent === undefined || seen.has(parent)) continue;
        seen.add(parent);
        for (const descendant of children.get(parent) ?? []) {
          if (!permitted(descendant)) return false;
          queue.push(descendant.pid);
        }
      }
      const launcher = processes.find((row) => row.pid === launcherPid);
      const child = processes.find((row) => row.pid === childPid);
      if (
        launcher?.group !== launcherPid ||
        child?.group !== launcherPid ||
        launcher.uid !== io.uid ||
        child.uid !== io.uid ||
        child.parent !== launcherPid
      )
        return false;
      return processes.filter((row) => row.group === launcherPid).every(permitted);
    }
    return false;
  } catch {
    return false;
  }
}
