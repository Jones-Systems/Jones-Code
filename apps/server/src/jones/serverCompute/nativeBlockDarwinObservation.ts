// @effect-diagnostics nodeBuiltinImport:off globalTimers:off -- The synthetic blocked-open observer must run in the compute child's own address space.
import * as NodeFS from "node:fs";
import * as NodeModule from "node:module";
import * as NodePath from "node:path";
import * as NodeWorkerThreads from "node:worker_threads";
import * as HostProcess from "@t3tools/shared/HostProcess";
import type { Bootstrap } from "./protocol.ts";

const observerSource = String.raw`
const { lstatSync, realpathSync, writeFileSync } = require("node:fs");
const { basename, dirname, join } = require("node:path");
const { workerData } = require("node:worker_threads");
const { thread, mainTid, directory, ffiEntry, subject } = workerData;
const { DataType: t, open, load } = require(ffiEntry);
const library = "qualification-darwin-mach";
open({ library, path: "/usr/lib/libSystem.B.dylib" });
const call = (funcName, retType, paramsType = [], paramsValue = []) =>
  load({ library, funcName, retType, paramsType, paramsValue });

const task = call("task_self_trap", t.U32);
const state = Buffer.alloc(272);
const stateCount = Buffer.alloc(4);
const basicInfo = Buffer.alloc(40);
const basicCount = Buffer.alloc(4);
// All generated names have the same byte length within this short-lived case.
const bytes = Buffer.alloc(Buffer.byteLength(join(
  directory, "native-block+" + Math.floor(Date.now() / 1000) + "+" + process.pid + ".log"
)) + 1);
const received = Buffer.alloc(8);
const validDirectory = () => {
  const stat = lstatSync(directory);
  return stat.isDirectory() && stat.uid === process.getuid() &&
    (stat.mode & 0o777) === 0o700 && realpathSync(directory) === directory;
};

function snapshot() {
  basicCount.writeUInt32LE(10); // THREAD_BASIC_INFO_COUNT
  stateCount.writeUInt32LE(68); // ARM_THREAD_STATE64_COUNT
  if (call("thread_info", t.I32, [t.U32, t.I32, t.U8Array, t.U8Array],
       [thread, 3, basicInfo, basicCount]) !== 0 ||
      basicCount.readUInt32LE() !== 10 || basicInfo.readUInt32LE(24) !== 3 || // TH_STATE_WAITING
      call("thread_get_state", t.I32, [t.U32, t.I32, t.U8Array, t.U8Array],
       [thread, 6, state, stateCount]) !== 0 || stateCount.readUInt32LE() !== 68) return;
  const address = state.readBigUInt64LE(0);  // x0, pathname
  const flags = state.readBigUInt64LE(8);    // x1, open flags
  const ordinal = state.readBigUInt64LE(128); // x16, syscall number
  const pc = state.readBigUInt64LE(256);
  // Darwin arm64 open / open_nocancel. Preserve write-only, append/create, blocking semantics.
  if (address < 4096n || pc === 0n || (ordinal !== 5n && ordinal !== 398n) ||
      (flags & 0x20fn) !== 0x209n) return;

  received.fill(0);
  bytes.fill(0);
  if (call("mach_vm_read_overwrite", t.I32, [t.U32, t.BigInt, t.BigInt, t.U8Array, t.U8Array],
       [task, address, BigInt(bytes.length), bytes, received]) !== 0 ||
      received.readBigUInt64LE() !== BigInt(bytes.length) || bytes.indexOf(0) !== bytes.length - 1) return;
  const path = bytes.toString("utf8", 0, bytes.length - 1);
  if (dirname(path) !== directory || !/^native-block\+[0-9]+\+[0-9]+\.log$/.test(basename(path)) ||
      !path.endsWith("+" + process.pid + ".log")) return;
  const fifo = lstatSync(path);
  if (!fifo.isFIFO() || fifo.uid !== process.getuid() || (fifo.mode & 0o777) !== 0o600) return;
  return { path, device: fifo.dev, inode: fifo.ino, flags: flags.toString(), ordinal: Number(ordinal),
    pc: pc.toString(16), runState: 3 };
}

function loadedImages() {
  const count = call("_dyld_image_count", t.U32);
  if (count === 0 || count > 4096) return [];
  const images = [];
  for (let i = 0; i < count; i++) {
    const path = call("_dyld_get_image_name", t.String, [t.U32], [i]);
    if (typeof path !== "string" || !/\/(?:libfff_c\.dylib|ffi-rs\.darwin-arm64\.node)$/.test(path)) continue;
    const base = call("_dyld_get_image_header", t.BigInt, [t.U32], [i]);
    images.push({ path, base: base.toString(16) });
  }
  return images;
}

if (task > 0 && validDirectory()) {
  const deadline = performance.now() + 8000;
  let before;
  const timer = setInterval(() => {
    if (performance.now() > deadline) { clearInterval(timer); return; }
    try {
      const observation = snapshot();
      if (!observation) { before = undefined; return; }
      const after = { at: Date.now(), observation };
      if (before && JSON.stringify(before.observation) === JSON.stringify(observation) &&
          after.at > before.at && validDirectory()) {
        const images = loadedImages();
        if (images.length === 2) {
          writeFileSync(join(directory, "native-block-observation-" + process.pid + ".json"),
            JSON.stringify({ subject, mainTid, before, after, images }) + "\n",
            { flag: "wx", mode: 0o600 });
        }
        clearInterval(timer);
      }
      before = after;
    } catch {
      // Missing, ambiguous, or unavailable evidence stays unknown.
      clearInterval(timer);
    }
  }, 25);
}
`;

/** Read only this process's main thread, and only for the synthetic blocked-open request. */
export function startDarwinNativeBlockObservation(
  logFileHint: string,
  bootstrap: Bootstrap,
  requestId: number,
): NodeWorkerThreads.Worker | undefined {
  if (
    HostProcess.Platform.defaultValue() !== "darwin" ||
    HostProcess.Architecture.defaultValue() !== "arm64" ||
    !NodeWorkerThreads.isMainThread
  )
    return;
  try {
    const directory = NodePath.dirname(logFileHint);
    const stat = NodeFS.lstatSync(directory);
    if (
      !stat.isDirectory() ||
      stat.uid !== process.getuid?.() ||
      (stat.mode & 0o777) !== 0o700 ||
      NodeFS.realpathSync(directory) !== directory
    )
      return;

    const require = NodeModule.createRequire(import.meta.url);
    const ffiEntry = require.resolve("ffi-rs");
    const { DataType, open, define } = require("ffi-rs") as typeof import("ffi-rs");
    const library = "qualification-darwin-main-thread";
    open({ library, path: "/usr/lib/libSystem.B.dylib" });
    const { pthread_self, pthread_mach_thread_np, pthread_threadid_np } = define({
      pthread_self: { library, retType: DataType.External, paramsType: [] },
      pthread_mach_thread_np: {
        library,
        retType: DataType.U32,
        paramsType: [DataType.External],
      },
      pthread_threadid_np: {
        library,
        retType: DataType.I32,
        paramsType: [DataType.External, DataType.U8Array],
      },
    });
    const pthread = pthread_self([]);
    const thread = pthread_mach_thread_np([pthread]);
    const id = Buffer.alloc(8);
    if (thread === 0 || pthread_threadid_np([pthread, id]) !== 0) return;
    const mainTid = id.readBigUInt64LE().toString();
    if (mainTid === "0") return;

    const worker = new NodeWorkerThreads.Worker(observerSource, {
      eval: true,
      workerData: {
        thread,
        mainTid,
        directory,
        ffiEntry,
        subject: {
          pid: process.pid,
          build: bootstrap.build,
          authorityId: bootstrap.authorityId,
          slotId: bootstrap.slotId,
          generation: bootstrap.slotGeneration,
          nonce: bootstrap.nonce,
          requestId,
        },
      },
    });
    worker.on("error", () => {});
    worker.unref();
    return worker;
  } catch {
    return undefined;
  }
}
