import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { constants } from "node:fs";
import { open, readFile, writeFile, lstat, realpath } from "node:fs/promises";
import { basename, join } from "node:path";
import { createHash } from "node:crypto";
import { setTimeout as pause } from "node:timers/promises";
import { parseLinuxSyscall, parseLinuxWitness, parseMacSample } from "./native-block-proof.mjs";
const execute = promisify(execFile);
const digest = (bytes) => createHash("sha256").update(bytes).digest("hex");
export const parserSha256 = digest(
  await readFile(new URL("./native-block-proof.mjs", import.meta.url)),
);
export const observerSha256 = digest(await readFile(new URL(import.meta.url)));
async function boundedRead(path, limit) {
  const file = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW);
  try {
    const buffer = Buffer.alloc(limit + 1);
    const { bytesRead } = await file.read(buffer, 0, buffer.length, 0);
    if (bytesRead > limit) throw new Error("OS evidence exceeds bound");
    return buffer.subarray(0, bytesRead).toString("utf8");
  } finally {
    await file.close();
  }
}
const identity = (stat) => ({ device: stat.dev, inode: stat.ino, mode: stat.mode & 0o777 });
export async function prepareNativeBlockFifos(directory, pid) {
  const stat = await lstat(directory);
  if (
    !Number.isSafeInteger(pid) ||
    pid <= 0 ||
    !stat.isDirectory() ||
    stat.isSymbolicLink() ||
    stat.uid !== process.getuid?.() ||
    (stat.mode & 0o777) !== 0o700 ||
    (await realpath(directory)) !== directory
  )
    throw new Error("Unowned FIFO directory");
  const second = Math.floor(Date.now() / 1000);
  const paths = Array.from({ length: 20 }, (_, i) =>
    join(directory, `native-block+${second - 2 + i}+${pid}.log`),
  );
  await execute("mkfifo", ["-m", "600", ...paths], { timeout: 2000, maxBuffer: 16_384 });
  const table = [];
  for (const path of paths) {
    const fifo = await lstat(path);
    if (!fifo.isFIFO() || fifo.uid !== process.getuid?.() || (fifo.mode & 0o777) !== 0o600)
      throw new Error("Not a private FIFO");
    table.push({ path, ...identity(fifo) });
  }
  return table;
}
export async function nativeBlockFifosUnchanged(table) {
  if (!Array.isArray(table) || table.length !== 20) return false;
  try {
    for (const item of table) {
      const stat = await lstat(item.path);
      if (
        !stat.isFIFO() ||
        stat.uid !== process.getuid?.() ||
        stat.dev !== item.device ||
        stat.ino !== item.inode ||
        (stat.mode & 0o777) !== 0o600
      )
        return false;
    }
    return true;
  } catch {
    return false;
  }
}
function processStat(text) {
  const end = text.lastIndexOf(")");
  if (end < 0) throw new Error("Malformed captured process stat");
  const fields = text
    .slice(end + 2)
    .trim()
    .split(/\s+/);
  const ppid = Number(fields[1]),
    startTimeTicks = Number(fields[19]);
  if (!Number.isSafeInteger(ppid) || !Number.isSafeInteger(startTimeTicks) || startTimeTicks <= 0)
    throw new Error("Missing process identity");
  return { state: fields[0] + " ", ppid, startTimeTicks };
}
function linuxImages(maps) {
  const paths = [
    ...new Set(
      maps
        .split("\n")
        .map((line) => /^\S+\s+\S+\s+\S+\s+\S+\s+\d+\s+(\/.*)$/.exec(line)?.[1])
        .filter(Boolean),
    ),
  ];
  const fff = paths.filter((path) => /^libfff(?:_c)?\.so$/.test(basename(path)));
  const ffi = paths.filter((path) => /ffi[_-]rs.*\.node$/.test(basename(path)));
  if (fff.length !== 1 || ffi.length !== 1) throw new Error("Unattributed loaded native images");
  return { fff: fff[0], ffi: ffi[0] };
}
async function imageHashes(paths) {
  const result = {};
  for (const [name, path] of Object.entries(paths)) {
    const stat = await lstat(path);
    if (!stat.isFile() || stat.isSymbolicLink() || stat.size <= 0 || stat.size > 128 * 1024 * 1024)
      throw new Error("Unhashable loaded image");
    result[name] = digest(await readFile(path));
  }
  return result;
}
// Only the grandparent fixture supplies this captured child binding. No CLI accepts arbitrary PIDs.
export async function witnessNativeBlock({ subject, capturedParentPid, table, artifact }) {
  const started = Date.now();
  let raw, proof, binaryPaths, binaryDigests, reason;
  try {
    if (
      !Number.isSafeInteger(capturedParentPid) ||
      !Number.isSafeInteger(subject.pid) ||
      capturedParentPid <= 0 ||
      subject.pid <= 0 ||
      !(await nativeBlockFifosUnchanged(table))
    )
      throw new Error("Missing captured synthetic binding");
    if (process.platform === "linux" && process.arch === "x64") {
      const base = `/proc/${subject.pid}`,
        task = `${base}/task/${subject.pid}`;
      const stat = processStat(await boundedRead(`${base}/stat`, 4096));
      const status = await boundedRead(`${base}/status`, 16_384);
      const uid = /^Uid:\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)$/m.exec(status);
      if (
        stat.ppid !== capturedParentPid ||
        !uid ||
        uid.slice(1).some((id) => Number(id) !== process.getuid?.())
      )
        throw new Error("Process is not the captured synthetic child");
      for (let attempt = 0; attempt < 12; attempt++) {
        const syscallBefore = await boundedRead(`${task}/syscall`, 1024);
        const syscall = parseLinuxSyscall(syscallBefore);
        if (!syscall) {
          await pause(25);
          continue;
        }
        const memory = await open(`${base}/mem`, constants.O_RDONLY);
        let pathname;
        try {
          const bytes = Buffer.alloc(4097);
          const { bytesRead } = await memory.read(bytes, 0, bytes.length, syscall.pointer);
          const nul = bytes.subarray(0, bytesRead).indexOf(0);
          if (nul < 1 || nul > 4096) throw new Error("Unbounded syscall pathname");
          pathname = bytes.subarray(0, nul).toString("utf8");
        } finally {
          await memory.close();
        }
        const after = processStat(await boundedRead(`${base}/stat`, 4096));
        raw = {
          pid: subject.pid,
          tid: subject.pid,
          syscallBefore,
          syscallAfter: await boundedRead(`${task}/syscall`, 1024),
          pathname,
          state: after.state,
          wchan: await boundedRead(`${task}/wchan`, 1024),
          startTimeTicks: stat.startTimeTicks,
          startTimeTicksAfter: after.startTimeTicks,
          maps: await boundedRead(`${base}/maps`, 2 * 1024 * 1024),
          uid: uid[1],
          parentPid: stat.ppid,
        };
        proof = parseLinuxWitness(
          raw,
          subject,
          table.map((item) => item.path),
        );
        if (proof) {
          binaryPaths = linuxImages(raw.maps);
          break;
        }
        await pause(25);
      }
      if (!proof) throw new Error("No exact main-TID blocked FIFO syscall witness");
    } else if (process.platform === "darwin") {
      // sample is bounded and its exit is awaited: no kill proof while sampling has threads suspended.
      await writeFile(artifact + ".sample", "", { flag: "wx", mode: 0o600 });
      await execute(
        "/usr/bin/sample",
        [String(subject.pid), "1", "1", "-file", artifact + ".sample"],
        { timeout: 4000, killSignal: "SIGKILL", maxBuffer: 64 * 1024 },
      );
      const sample = await boundedRead(artifact + ".sample", 4 * 1024 * 1024);
      const stack = parseMacSample(sample, subject.pid);
      raw = {
        sample,
        sampleArtifact: artifact + ".sample",
        sampleSha256: digest(await readFile(artifact + ".sample")),
        samplerFinished: true,
        parsed: stack ?? null,
      };
      const images = sample.split(/^Binary Images:\s*$/m)[1] ?? "";
      const paths = images
        .split("\n")
        .map((line) => /\s(\/[^\n]+)$/.exec(line)?.[1])
        .filter(Boolean);
      const fff = paths.filter((path) => /^libfff(?:_c)?\.dylib$/.test(basename(path)));
      const ffi = paths.filter((path) => /ffi[_-]rs.*\.node$/.test(basename(path)));
      if (fff.length === 1 && ffi.length === 1) {
        binaryPaths = { fff: fff[0], ffi: ffi[0] };
        binaryDigests = await imageHashes(binaryPaths);
        raw.binaryPaths = binaryPaths;
        raw.binaryDigests = binaryDigests;
      }
      if (!stack)
        throw new Error("Missing attributable main-thread fff create/log-init/open chain");
      // sample identifies the phase but does not expose open(2)'s pathname argument.
      // Do not choose one of the twenty possible siblings by timing or frame-name inference.
      throw new Error(
        "macOS sample does not establish the exact generated FIFO pathname; receipt remains unknown",
      );
    } else throw new Error("Unsupported qualification observer platform");
    binaryDigests = await imageHashes(binaryPaths);
    const finished = Date.now();
    const evidence = {
      schema: "jones.native-block-witness/v1",
      started,
      finished,
      subject,
      raw,
      proof,
      binaryPaths,
      binaryDigests,
      parserSha256,
      observerSha256,
    };
    const bytes = JSON.stringify(evidence, null, 2) + "\n";
    await writeFile(artifact, bytes, { flag: "wx", mode: 0o600 });
    return {
      outcome: true,
      ...proof,
      finished,
      binaryPaths,
      binaryDigests,
      witness: {
        method: process.platform === "linux" ? "linux-syscall-path" : "macos-native-stack",
        artifact,
        sha256: digest(bytes),
        parserSha256,
        observerSha256,
      },
    };
  } catch (cause) {
    reason = cause instanceof Error ? cause.message : "unavailable observer";
    const finished = Date.now();
    const bytes =
      JSON.stringify(
        {
          schema: "jones.native-block-witness/v1",
          started,
          finished,
          subject,
          raw: raw ?? null,
          outcome: "unknown",
          reason,
          parserSha256,
          observerSha256,
        },
        null,
        2,
      ) + "\n";
    await writeFile(artifact, bytes, { flag: "wx", mode: 0o600 });
    return {
      outcome: "unknown",
      reason,
      artifact,
      finished,
      binaryPaths,
      binaryDigests,
      witness: {
        method: process.platform === "linux" ? "linux-syscall-path" : "macos-native-stack",
        artifact,
        sha256: digest(bytes),
        parserSha256,
        observerSha256,
      },
    };
  }
}

// This is used only by the live-parent negative control after a successful exact-path witness.
export async function releaseNativeBlockFifo(table, path) {
  if (!table.some((item) => item.path === path) || !(await nativeBlockFifosUnchanged(table)))
    throw new Error("Cannot release an unproved FIFO");
  return open(path, constants.O_RDONLY | constants.O_NONBLOCK | constants.O_NOFOLLOW);
}
