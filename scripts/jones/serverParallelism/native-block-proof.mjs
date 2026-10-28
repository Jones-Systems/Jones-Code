// These parsers accept OS observations, never JS entry markers, CPU deltas or test exit codes.
export const NATIVE_BLOCK_PHASE = "fff-create/log-init/fifo-open";
export const NATIVE_BLOCK_SCHEMA = "jones.native-block-proof/v1";
const record = (value) => value !== null && typeof value === "object" && !Array.isArray(value);
const closed = (value, fields) => record(value) && Object.keys(value).length === fields.length &&
  fields.every((field) => Object.hasOwn(value, field));
const positive = (value) => Number.isSafeInteger(value) && value > 0;
const hash = (value) => typeof value === "string" && /^[a-f0-9]{64}$/.test(value);
const identifier = (value) => typeof value === "string" && /^[a-zA-Z0-9_-]{1,128}$/.test(value);
const path = (value) => typeof value === "string" && value.startsWith("/") &&
  value.length <= 4096 && !/[\x00-\x1f\x7f]/.test(value);
const tri = (value) => value === true || value === false || value === "unknown";

export function parseLinuxSyscall(text) {
  if (typeof text !== "string" || text.length > 1024) return undefined;
  const values = text.trim().split(/\s+/);
  if (values.length !== 9 || !/^\d+$/.test(values[0]) ||
      !values.slice(1).every((value) => /^0x[0-9a-f]+$/i.test(value))) return undefined;
  const number = Number(values[0]), args = values.slice(1, 7).map((value) => BigInt(value));
  if (number !== 2 && number !== 257) return undefined; // Linux x64 open / openat only.
  const pointer = args[number === 2 ? 0 : 1], flags = args[number === 2 ? 1 : 2];
  // The pinned first log open is append/create/write, with a blocking FIFO handshake.
  if ((flags & 3n) !== 1n || (flags & 0x800n) !== 0n ||
      (flags & 0x400n) === 0n || (flags & 0x40n) === 0n || pointer <= 0n ||
      pointer > BigInt(Number.MAX_SAFE_INTEGER)) return undefined;
  return { number, pointer: Number(pointer), flags: flags.toString() };
}

export function parseLinuxWitness(raw, subject, fifoPaths) {
  if (!record(raw) || raw.pid !== subject.pid || raw.tid !== subject.pid ||
      raw.syscallBefore !== raw.syscallAfter || !parseLinuxSyscall(raw.syscallBefore) ||
      !Array.isArray(fifoPaths) || !fifoPaths.includes(raw.pathname) ||
      typeof raw.state !== "string" || !/^S\s+/.test(raw.state) ||
      typeof raw.wchan !== "string" || !/^wait_for_partner$/.test(raw.wchan.trim()) || !positive(raw.startTimeTicks) ||
      raw.startTimeTicks !== raw.startTimeTicksAfter) return undefined;
  return { mainTid: String(subject.pid), fifoPath: raw.pathname };
}

export function parseMacSample(text, pid) {
  if (!positive(pid) || typeof text !== "string" || text.length > 4 * 1024 * 1024 ||
      !new RegExp(`^Process:\\s+.+ \\[${pid}\\]\\s*$`, "m").test(text)) return undefined;
  const tree = text.split(/^Call graph:\s*$/m)[1]?.split(/^Total number in stack/m)[0];
  if (!tree) return undefined;
  // Require an explicitly labelled main thread and a single nested chain, not matches across threads.
  const sections = tree.split(/(?=^\s*\d+ Thread_)/m);
  const main = sections.filter((section) => /^\s*\d+ Thread_\S+.*(?:com\.apple\.main-thread|\(main\))/m.test(section));
  if (main.length !== 1) return undefined;
  const tid = /^\s*\d+ Thread_(\S+)/m.exec(main[0])?.[1];
  const rows = main[0].split("\n").filter((line) => /\(in .+\)/.test(line));
  const create = rows.findIndex((line) => /\bfff_create_instance_with\b/.test(line));
  const init = rows.findIndex((line, i) => i > create && /(?:fff.*log.*init_tracing|init_tracing.*fff)/.test(line));
  const log = rows.findIndex((line, i) => i > init && /(?:sigsegv.*set_log_fd|set_log_fd.*sigsegv)/.test(line));
  const open = rows.findIndex((line, i) => i > log && /\b(?:__open|__open_nocancel|open)\b/.test(line));
  if (!tid || create < 0 || init < 0 || log < 0 || open < 0) return undefined;
  // No branch may occur between the native creation and blocking-open frames.
  const depth = (line) => line.indexOf(line.trimStart().replace(/^[+!:| ]*/, ""));
  if (rows.slice(create, open + 1).some((line, i, all) => i > 0 && depth(line) <= depth(all[i - 1]))) return undefined;
  const images = [create, init, log].map((i) => /\(in ([^)]+)\)/.exec(rows[i])?.[1]);
  if (images.some((name) => name !== images[0]) || !/^libfff(?:[._-].*)?$/.test(images[0] ?? "")) return undefined;
  return { mainTid: tid, image: images[0] };
}

export function nativeBlockReceiptPredicate(value, expected) {
  if (!record(expected) || !["linux-x64", "macos-arm64"].includes(expected.os) ||
      !["source", "bundle", "electron", "sea"].includes(expected.form) ||
      !["laptop", "vps"].includes(expected.profile) || !hash(expected.build)) return "unknown";
  if (!closed(value, ["schema", "phase", "os", "form", "profile", "subject", "binaryDigests",
    "binaryPaths", "witness", "blockerHeldThroughKill", "parentCapturedSigkill", "childrenAbsent",
    "sameJournalRecovered", "replacementUseful", "blocker", "timeline", "allocation", "guardian",
    "negativeControl", "noWitnessRejected", "cleanupProved"]) || value.schema !== NATIVE_BLOCK_SCHEMA ||
    value.phase !== NATIVE_BLOCK_PHASE || value.os !== expected.os || value.form !== expected.form ||
    value.profile !== expected.profile || !closed(value.subject, ["pid", "mainTid", "build", "authorityId",
      "slotId", "generation", "nonce", "requestId"]) || !positive(value.subject.pid) ||
    typeof value.subject.mainTid !== "string" || !/^(?:\d+|0x[0-9a-f]+)$/.test(value.subject.mainTid) ||
    (value.os === "linux-x64" && value.subject.mainTid !== String(value.subject.pid)) ||
    !hash(value.subject.build) || value.subject.build !== expected.build ||
    !identifier(value.subject.authorityId) || !identifier(value.subject.nonce) ||
    !Number.isInteger(value.subject.slotId) || value.subject.slotId < 0 ||
    value.subject.slotId >= (value.profile === "vps" ? 3 : 2) ||
    !positive(value.subject.generation) || !positive(value.subject.requestId) ||
    !closed(value.binaryDigests, ["fff", "ffi"]) || !Object.values(value.binaryDigests).every(hash) ||
    !closed(value.binaryPaths, ["fff", "ffi"]) || !Object.values(value.binaryPaths).every(path) ||
    !closed(value.witness, ["method", "artifact", "sha256", "parserSha256", "observerSha256"]) ||
    value.witness.method !== (value.os === "linux-x64" ? "linux-syscall-path" : "macos-native-stack") ||
    !path(value.witness.artifact) || ![value.witness.sha256, value.witness.parserSha256, value.witness.observerSha256].every(hash) ||
    !closed(value.blocker, ["fifoPath", "device", "inode", "mode", "preparedNames", "noReader", "unchanged"]) ||
    !path(value.blocker.fifoPath) || !positive(value.blocker.inode) ||
    !Number.isSafeInteger(value.blocker.device) || value.blocker.device < 0 || value.blocker.mode !== 0o600 ||
    value.blocker.preparedNames !== 20 || !tri(value.blocker.noReader) || !tri(value.blocker.unchanged) ||
    !closed(value.timeline, ["requestIssued", "witnessFinished", "guardianAfterWitness", "lastHeldCheck", "kill", "armExpires"]) ||
    !Object.values(value.timeline).every(positive) ||
    !(value.timeline.requestIssued <= value.timeline.witnessFinished &&
      value.timeline.witnessFinished < value.timeline.guardianAfterWitness &&
      value.timeline.guardianAfterWitness <= value.timeline.lastHeldCheck &&
      value.timeline.lastHeldCheck <= value.timeline.kill && value.timeline.kill < value.timeline.armExpires &&
      value.timeline.armExpires - value.timeline.requestIssued <= 30_000) ||
    !closed(value.allocation, ["attempt", "limit", "registeredChildren", "outstandingThroughKill"]) ||
    !positive(value.allocation.attempt) || value.allocation.attempt > 4 || value.allocation.limit !== 4 ||
    !Array.isArray(value.allocation.registeredChildren) ||
    value.allocation.registeredChildren.length !== (value.profile === "vps" ? 3 : 2) ||
    !value.allocation.registeredChildren.every(positive) ||
    new Set(value.allocation.registeredChildren).size !== value.allocation.registeredChildren.length ||
    !value.allocation.registeredChildren.includes(value.subject.pid) ||
    !tri(value.allocation.outstandingThroughKill) ||
    !closed(value.guardian, ["postWitnessResources", "generationUnchanged"]) ||
    !Object.values(value.guardian).every(tri) ||
    !closed(value.negativeControl, ["witnessed", "exactFifoReleased", "normalResult", "cleanRetirement"]) ||
    !Object.values(value.negativeControl).every(tri)) return "unknown";
  const observations = [value.blockerHeldThroughKill, value.parentCapturedSigkill, value.childrenAbsent,
    value.sameJournalRecovered, value.replacementUseful, value.blocker.noReader, value.blocker.unchanged,
    value.allocation.outstandingThroughKill, ...Object.values(value.guardian),
    ...Object.values(value.negativeControl), value.noWitnessRejected, value.cleanupProved];
  if (!observations.every(tri)) return "unknown";
  return observations.includes(false) ? false : observations.every((field) => field === true) ? true : "unknown";
}
