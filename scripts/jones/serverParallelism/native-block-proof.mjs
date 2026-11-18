import * as NodeCrypto from "node:crypto";

// These parsers accept OS observations, never JS entry markers, CPU deltas or test exit codes.
export const NATIVE_BLOCK_PHASE = "fff-create/log-init/fifo-open";
export const NATIVE_BLOCK_SCHEMA = "jones.native-block-proof/v1";
const record = (value) => value !== null && typeof value === "object" && !Array.isArray(value);
const closed = (value, fields) =>
  record(value) &&
  Object.keys(value).length === fields.length &&
  fields.every((field) => Object.hasOwn(value, field));
const positive = (value) => Number.isSafeInteger(value) && value > 0;
const hash = (value) => typeof value === "string" && /^[a-f0-9]{64}$/.test(value);
const identifier = (value) => typeof value === "string" && /^[a-zA-Z0-9_-]{1,128}$/.test(value);
const path = (value) =>
  typeof value === "string" &&
  value.startsWith("/") &&
  value.length <= 4096 &&
  !/[\x00-\x1f\x7f]/.test(value);
const tri = (value) => value === true || value === false || value === "unknown";

// Admission uses retirement evidence independently of whether the native proof passed.
export function nativeBlockCohortRetired(cohort, count) {
  if (
    ![2, 3].includes(count) ||
    cohort?.cleanupProved !== true ||
    cohort.childrenAbsent !== true ||
    cohort.parentExit?.captured !== true ||
    !Array.isArray(cohort.candidate?.children) ||
    cohort.candidate.children.length !== count ||
    !cohort.candidate.children.every(positive) ||
    new Set(cohort.candidate.children).size !== count
  )
    return false;
  return (
    cohort.cleanupRecovery === true ||
    (cohort.normalResult === true &&
      cohort.parentExit.code === 0 &&
      cohort.parentExit.signal === null) ||
    (cohort.sameJournalRecovered === true && cohort.replacementUseful === true)
  );
}

export function parseLinuxSyscall(text) {
  if (typeof text !== "string" || text.length > 1024) return undefined;
  const values = text.trim().split(/\s+/);
  if (
    values.length !== 9 ||
    !/^\d+$/.test(values[0]) ||
    !values.slice(1).every((value) => /^0x[0-9a-f]+$/i.test(value))
  )
    return undefined;
  const number = Number(values[0]),
    args = values.slice(1, 7).map((value) => BigInt(value));
  if (number !== 2 && number !== 257) return undefined; // Linux x64 open / openat only.
  const pointer = args[number === 2 ? 0 : 1],
    flags = args[number === 2 ? 1 : 2];
  // The pinned first log open is append/create/write, with a blocking FIFO handshake.
  if (
    (flags & 3n) !== 1n ||
    (flags & 0x800n) !== 0n ||
    (flags & 0x400n) === 0n ||
    (flags & 0x40n) === 0n ||
    pointer <= 0n ||
    pointer > BigInt(Number.MAX_SAFE_INTEGER)
  )
    return undefined;
  return { number, pointer: Number(pointer), flags: flags.toString() };
}

export function parseLinuxWitness(raw, subject, fifoPaths) {
  if (
    !record(raw) ||
    raw.pid !== subject.pid ||
    raw.tid !== subject.pid ||
    raw.syscallBefore !== raw.syscallAfter ||
    !parseLinuxSyscall(raw.syscallBefore) ||
    !Array.isArray(fifoPaths) ||
    !fifoPaths.includes(raw.pathname) ||
    typeof raw.state !== "string" ||
    !/^S\s+/.test(raw.state) ||
    typeof raw.wchan !== "string" ||
    !/^wait_for_partner$/.test(raw.wchan.trim()) ||
    !positive(raw.startTimeTicks) ||
    raw.startTimeTicks !== raw.startTimeTicksAfter
  )
    return undefined;
  return { mainTid: String(subject.pid), fifoPath: raw.pathname };
}

function macMainThread(text, pid) {
  if (
    !positive(pid) ||
    typeof text !== "string" ||
    text.length > 4 * 1024 * 1024 ||
    !new RegExp(`^Process:\\s+.+ \\[${pid}\\]\\s*$`, "m").test(text)
  )
    return undefined;
  const tree = text.split(/^Call graph:\s*$/m)[1]?.split(/^Total number in stack/m)[0];
  if (!tree) return undefined;
  // Require an explicitly labelled main thread and a single nested chain, not matches across threads.
  const sections = tree.split(/(?=^\s*\d+ Thread_)/m);
  const main = sections.filter((section) =>
    /^\s*\d+ Thread_\S+.*(?:com\.apple\.main-thread|\(main\))/m.test(section),
  );
  if (main.length !== 1) return undefined;
  const tid = /^\s*\d+ Thread_(\S+)/m.exec(main[0])?.[1];
  return tid ? { tid, section: main[0] } : undefined;
}

export function parseMacSample(text, pid) {
  const main = macMainThread(text, pid);
  if (!main) return undefined;
  const rows = main.section.split("\n").filter((line) => /\(in .+\)/.test(line));
  const create = rows.findIndex((line) => /\bfff_create_instance_with\b/.test(line));
  const init = rows.findIndex(
    (line, i) => i > create && /(?:fff.*log.*init_tracing|init_tracing.*fff)/.test(line),
  );
  const log = rows.findIndex(
    (line, i) => i > init && /(?:sigsegv.*set_log_fd|set_log_fd.*sigsegv)/.test(line),
  );
  const open = rows.findIndex(
    (line, i) => i > log && /\b(?:__open|__open_nocancel|open)\b/.test(line),
  );
  if (create < 0 || init < 0 || log < 0 || open < 0) return undefined;
  // No branch may occur between the native creation and blocking-open frames.
  const depth = (line) => line.indexOf(line.trimStart().replace(/^[+!:| ]*/, ""));
  if (
    rows.slice(create, open + 1).some((line, i, all) => i > 0 && depth(line) <= depth(all[i - 1]))
  )
    return undefined;
  const images = [create, init, log].map((i) => /\(in ([^)]+)\)/.exec(rows[i])?.[1]);
  if (images.some((name) => name !== images[0]) || !/^libfff(?:[._-].*)?$/.test(images[0] ?? ""))
    return undefined;
  return { mainTid: main.tid, image: images[0] };
}

// The pinned arm64 library strips two frames between fff_create_instance_with and _open.
// In this build the preceding BL instructions at 0x39a2c, 0x1c54a4 and 0x2cf7b0 form
// the observed chain. The final destination, 0x3e4b44, is the _open symbol stub.
const darwinFff = {
  name: "libfff_c.dylib",
  codeSha256: "040dff319ec3c6d2c015801dee0bb6f912de2d215749c9638c0dbca10e9331d2",
  uuid: "A16BF197-B7BB-31BF-9522-B9F734296D65",
  returnAddresses: [0x39a30n, 0x1c54a8n, 0x2cf7b4n],
};
const darwinFfiName = "ffi-rs.darwin-arm64.node";
const decimal = (value) => typeof value === "string" && /^[0-9]{1,20}$/.test(value);
const hexadecimal = (value) => typeof value === "string" && /^[a-f0-9]{1,16}$/i.test(value);
const lowerHex = (value) => BigInt("0x" + value);

// The CLI archive signer replaces the terminal signature and updates its size and
// the two __LINKEDIT sizes. The load addresses, signature offset, code and imports
// stay in this fingerprint. The receipt also binds the hash of each complete file.
export function darwinFffCodeDigest(bytes) {
  const headerSize = 32;
  if (
    !Buffer.isBuffer(bytes) ||
    bytes.length < headerSize ||
    bytes.readUInt32LE(0) !== 0xfeedfacf || // MH_MAGIC_64
    bytes.readUInt32LE(4) !== 0x0100000c || // CPU_TYPE_ARM64
    bytes.readUInt32LE(12) !== 6 // MH_DYLIB
  )
    return undefined;
  const count = bytes.readUInt32LE(16);
  const commandEnd = headerSize + bytes.readUInt32LE(20);
  if (count > 256 || commandEnd > bytes.length) return undefined;
  let linkedit, signature;
  let offset = headerSize;
  for (let command = 0; command < count; command++) {
    if (offset + 8 > commandEnd) return undefined;
    const kind = bytes.readUInt32LE(offset);
    const size = bytes.readUInt32LE(offset + 4);
    if (size < 8 || size % 8 !== 0 || offset + size > commandEnd) return undefined;
    if (kind === 0x19) {
      // LC_SEGMENT_64
      if (size < 72) return undefined;
      const name = bytes.toString("ascii", offset + 8, offset + 24).replace(/\0+$/, "");
      if (name === "__LINKEDIT") {
        if (linkedit || size !== 72 || bytes.readUInt32LE(offset + 64) !== 0) return undefined;
        linkedit = {
          command: offset,
          vmSize: bytes.readBigUInt64LE(offset + 32),
          fileOffset: bytes.readBigUInt64LE(offset + 40),
          fileSize: bytes.readBigUInt64LE(offset + 48),
        };
      }
    } else if (kind === 0x1d) {
      // LC_CODE_SIGNATURE
      if (signature || size !== 16) return undefined;
      signature = {
        command: offset,
        fileOffset: bytes.readUInt32LE(offset + 8),
        size: bytes.readUInt32LE(offset + 12),
      };
    }
    offset += size;
  }
  if (
    offset !== commandEnd ||
    !linkedit ||
    !signature ||
    signature.size === 0 ||
    signature.fileOffset < commandEnd ||
    signature.fileOffset + signature.size !== bytes.length ||
    linkedit.fileOffset < BigInt(commandEnd) ||
    linkedit.fileOffset > BigInt(signature.fileOffset) ||
    linkedit.fileOffset + linkedit.fileSize !== BigInt(bytes.length) ||
    linkedit.vmSize < linkedit.fileSize
  )
    return undefined;
  const canonical = Buffer.from(bytes.subarray(0, signature.fileOffset));
  canonical.fill(0, linkedit.command + 32, linkedit.command + 40);
  canonical.fill(0, linkedit.command + 48, linkedit.command + 56);
  canonical.fill(0, signature.command + 12, signature.command + 16);
  return NodeCrypto.createHash("sha256").update(canonical).digest("hex");
}

function darwinImages(sample, observed, digests) {
  if (
    !Array.isArray(observed) ||
    observed.length !== 2 ||
    !hash(digests?.fff) ||
    !hash(digests.ffi)
  )
    return undefined;
  const images = sample.split(/^Binary Images:\s*$/m)[1]?.split("\n");
  if (!images) return undefined;
  const result = {};
  for (const [key, name] of [
    ["fff", darwinFff.name],
    ["ffi", darwinFfiName],
  ]) {
    const matches = observed.filter(
      (row) =>
        closed(row, ["path", "base"]) &&
        path(row.path) &&
        row.path.endsWith("/" + name) &&
        hexadecimal(row.base),
    );
    const shown = images
      .map((row) =>
        /^\s*0x([a-f0-9]+)\s+-\s+0x[a-f0-9]+\s+\+?(\S+).*?<([A-F0-9-]{36})>\s/i.exec(row),
      )
      .filter((row) => row?.[2] === name);
    if (
      matches.length !== 1 ||
      shown.length !== 1 ||
      lowerHex(matches[0].base) !== lowerHex(shown[0][1]) ||
      (key === "fff" && shown[0][3].toUpperCase() !== darwinFff.uuid)
    )
      return undefined;
    result[key] = matches[0];
  }
  return result;
}

function darwinStackAtOpen(text, pid, threadId, fffBase, pc) {
  const main = macMainThread(text, pid);
  if (main?.tid !== threadId) return false;
  const frames = main.section
    .split("\n")
    .map((line) => /^([\s+!:|]+)\d+\s+(\S.*?)\s+\(in ([^)]+)\).*?\[0x([a-f0-9]+)\]\s*$/i.exec(line))
    .filter(Boolean);
  const create = frames.findIndex(
    (frame) =>
      frame[2] === "fff_create_instance_with" &&
      frame[3] === darwinFff.name &&
      lowerHex(frame[4]) === fffBase + darwinFff.returnAddresses[0],
  );
  const chain = frames.slice(create, create + 5);
  if (create < 0 || chain.length !== 5) return false;
  return (
    darwinFff.returnAddresses.every(
      (offset, i) => chain[i][3] === darwinFff.name && lowerHex(chain[i][4]) === fffBase + offset,
    ) &&
    chain[3][2] === "open" &&
    /^__open(?:_nocancel)?$/.test(chain[4][2]) &&
    chain.slice(3).every((frame) => frame[3] === "libsystem_kernel.dylib") &&
    lowerHex(chain[4][4]) === pc &&
    chain.slice(1).every((frame, i) => frame[1].length > chain[i][1].length)
  );
}

export function parseDarwinWitness(raw, subject, fifoTable, bounds, binaryDigests) {
  const mach = raw?.mach;
  if (
    !positive(subject?.pid) ||
    raw?.samplerFinished !== true ||
    raw.fffCodeSha256 !== darwinFff.codeSha256 ||
    typeof raw.sample !== "string" ||
    raw.sample.length > 4 * 1024 * 1024 ||
    !closed(mach, ["subject", "mainTid", "before", "after", "images"]) ||
    !decimal(mach.mainTid) ||
    BigInt(mach.mainTid) === 0n ||
    !closed(mach.subject, [
      "pid",
      "build",
      "authorityId",
      "slotId",
      "generation",
      "nonce",
      "requestId",
    ]) ||
    !Object.keys(mach.subject).every((key) => mach.subject[key] === subject[key]) ||
    !closed(mach.before, ["at", "observation"]) ||
    !closed(mach.after, ["at", "observation"]) ||
    !positive(mach.before.at) ||
    !positive(mach.after.at) ||
    !positive(bounds?.issued) ||
    !positive(bounds?.finished) ||
    mach.before.at < bounds.issued ||
    mach.after.at <= mach.before.at ||
    mach.after.at > bounds.finished ||
    JSON.stringify(mach.before.observation) !== JSON.stringify(mach.after.observation)
  )
    return undefined;
  const observed = mach.after.observation;
  if (
    !closed(observed, ["path", "device", "inode", "flags", "ordinal", "pc", "runState"]) ||
    !path(observed.path) ||
    !decimal(observed.flags) ||
    (BigInt(observed.flags) & 0x20fn) !== 0x209n ||
    ![5, 398].includes(observed.ordinal) ||
    !hexadecimal(observed.pc) ||
    observed.runState !== 3 ||
    !Array.isArray(fifoTable) ||
    fifoTable.length !== 20 ||
    !fifoTable.some(
      (fifo) =>
        fifo.path === observed.path &&
        fifo.device === observed.device &&
        fifo.inode === observed.inode &&
        fifo.mode === 0o600,
    )
  )
    return undefined;
  const images = darwinImages(raw.sample, mach.images, binaryDigests);
  if (
    !images ||
    !darwinStackAtOpen(
      raw.sample,
      subject.pid,
      mach.mainTid,
      lowerHex(images.fff.base),
      lowerHex(observed.pc),
    )
  )
    return undefined;
  return {
    mainTid: mach.mainTid,
    fifoPath: observed.path,
    binaryPaths: { fff: images.fff.path, ffi: images.ffi.path },
  };
}

export function nativeBlockReceiptPredicate(value, expected) {
  if (
    !record(expected) ||
    !["linux-x64", "macos-arm64"].includes(expected.os) ||
    !["source", "bundle", "electron", "sea"].includes(expected.form) ||
    !["laptop", "vps"].includes(expected.profile) ||
    !hash(expected.build)
  )
    return "unknown";
  if (
    !closed(value, [
      "schema",
      "phase",
      "os",
      "form",
      "profile",
      "subject",
      "binaryDigests",
      "binaryPaths",
      "witness",
      "blockerHeldThroughKill",
      "parentCapturedSigkill",
      "childrenAbsent",
      "sameJournalRecovered",
      "replacementUseful",
      "blocker",
      "timeline",
      "allocation",
      "guardian",
      "negativeControl",
      "noWitnessRejected",
      "cleanupProved",
    ]) ||
    value.schema !== NATIVE_BLOCK_SCHEMA ||
    value.phase !== NATIVE_BLOCK_PHASE ||
    value.os !== expected.os ||
    value.form !== expected.form ||
    value.profile !== expected.profile ||
    !closed(value.subject, [
      "pid",
      "mainTid",
      "build",
      "authorityId",
      "slotId",
      "generation",
      "nonce",
      "requestId",
    ]) ||
    !positive(value.subject.pid) ||
    typeof value.subject.mainTid !== "string" ||
    !/^(?:\d+|0x[0-9a-f]+)$/.test(value.subject.mainTid) ||
    (value.os === "linux-x64" && value.subject.mainTid !== String(value.subject.pid)) ||
    !hash(value.subject.build) ||
    value.subject.build !== expected.build ||
    !identifier(value.subject.authorityId) ||
    !identifier(value.subject.nonce) ||
    !Number.isInteger(value.subject.slotId) ||
    value.subject.slotId < 0 ||
    value.subject.slotId >= (value.profile === "vps" ? 3 : 2) ||
    !positive(value.subject.generation) ||
    !positive(value.subject.requestId) ||
    !closed(value.binaryDigests, ["fff", "ffi"]) ||
    !Object.values(value.binaryDigests).every(hash) ||
    !closed(value.binaryPaths, ["fff", "ffi"]) ||
    !Object.values(value.binaryPaths).every(path) ||
    !closed(value.witness, ["method", "artifact", "sha256", "parserSha256", "observerSha256"]) ||
    value.witness.method !==
      (value.os === "linux-x64" ? "linux-syscall-path" : "macos-native-stack") ||
    !path(value.witness.artifact) ||
    ![value.witness.sha256, value.witness.parserSha256, value.witness.observerSha256].every(hash) ||
    !closed(value.blocker, [
      "fifoPath",
      "device",
      "inode",
      "mode",
      "preparedNames",
      "noReader",
      "unchanged",
    ]) ||
    !path(value.blocker.fifoPath) ||
    !positive(value.blocker.inode) ||
    !Number.isSafeInteger(value.blocker.device) ||
    value.blocker.device < 0 ||
    value.blocker.mode !== 0o600 ||
    value.blocker.preparedNames !== 20 ||
    !tri(value.blocker.noReader) ||
    !tri(value.blocker.unchanged) ||
    !closed(value.timeline, [
      "requestIssued",
      "witnessFinished",
      "guardianAfterWitness",
      "lastHeldCheck",
      "kill",
      "armExpires",
    ]) ||
    !Object.values(value.timeline).every(positive) ||
    !(
      value.timeline.requestIssued <= value.timeline.witnessFinished &&
      value.timeline.witnessFinished < value.timeline.guardianAfterWitness &&
      value.timeline.guardianAfterWitness <= value.timeline.lastHeldCheck &&
      value.timeline.lastHeldCheck <= value.timeline.kill &&
      value.timeline.kill < value.timeline.armExpires &&
      value.timeline.armExpires - value.timeline.requestIssued <= 30_000
    ) ||
    !closed(value.allocation, [
      "attempt",
      "limit",
      "registeredChildren",
      "outstandingThroughKill",
    ]) ||
    !positive(value.allocation.attempt) ||
    value.allocation.attempt > 4 ||
    value.allocation.limit !== 4 ||
    !Array.isArray(value.allocation.registeredChildren) ||
    value.allocation.registeredChildren.length !== (value.profile === "vps" ? 3 : 2) ||
    !value.allocation.registeredChildren.every(positive) ||
    new Set(value.allocation.registeredChildren).size !==
      value.allocation.registeredChildren.length ||
    !value.allocation.registeredChildren.includes(value.subject.pid) ||
    !tri(value.allocation.outstandingThroughKill) ||
    !closed(value.guardian, ["postWitnessResources", "generationUnchanged"]) ||
    !Object.values(value.guardian).every(tri) ||
    !closed(value.negativeControl, [
      "witnessed",
      "exactFifoReleased",
      "normalResult",
      "cleanRetirement",
    ]) ||
    !Object.values(value.negativeControl).every(tri)
  )
    return "unknown";
  const observations = [
    value.blockerHeldThroughKill,
    value.parentCapturedSigkill,
    value.childrenAbsent,
    value.sameJournalRecovered,
    value.replacementUseful,
    value.blocker.noReader,
    value.blocker.unchanged,
    value.allocation.outstandingThroughKill,
    ...Object.values(value.guardian),
    ...Object.values(value.negativeControl),
    value.noWitnessRejected,
    value.cleanupProved,
  ];
  if (!observations.every(tri)) return "unknown";
  return observations.includes(false)
    ? false
    : observations.every((field) => field === true)
      ? true
      : "unknown";
}
