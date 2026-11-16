// @effect-diagnostics nodeBuiltinImport:off preferSchemaOverJson:off -- Closed same-build private FD4 protocol.
import type * as NodeStream from "node:stream";
import {
  counter, decodeBootstrap, encodeFrame, frames, isToken, keys, record,
  sameBootstrap, writeEncodedFrame, type Bootstrap,
} from "./protocol.ts";
import type { FinderToken, WorkspaceKey } from "../serverParallelism/computeContract.ts";

// Auxiliary budgets are separate from the immutable compute/guardian limits.
export const PROBE_LIMITS = Object.freeze({
  active: 2, activePerSlot: 1, outstanding: 16, perToken: 4, perSlot: 8,
  admissionMs: 10_000, frameBytes: 65_536, chunkBytes: 32_768, credits: 2,
  writerBytes: 262_144, headerBytes: 32_768, paths: 4096, pathBytes: 16_384,
  stdinBytes: 262_144, uploadBytes: 2_097_152, uploadGlobalBytes: 4_194_304,
  descriptorBytes: 1_048_576, resultBytes: 67_108_864,
  grepBytes: 4_194_304, ignoreBytes: 16_777_216, repositoryBytes: 1_000_000,
  ownerProbes: 256, ownerStdinBytes: 8_388_608, ownerOutputBytes: 67_108_864,
  residentMs: 30_000, cancelReserveMs: 1000, cleanupMs: 5000,
});
export type ProbeOwner =
  | { readonly kind: "foreground"; readonly requestId: number }
  | { readonly kind: "scan"; readonly epoch: number };
export type SearchGitProbeInput =
  | { readonly kind: "ignoredPaths" }
  | { readonly kind: "grep"; readonly pattern: string; readonly regexMode: boolean;
      readonly ignoreCase: boolean; readonly onlyMatching: boolean }
  | { readonly kind: "ignoredCandidates"; readonly repository: string; readonly paths: readonly string[] }
  | { readonly kind: "repositoryPresence"; readonly repository: string };
export interface ProbeOutput {
  readonly exitCode: number;
  readonly stdout: string;
  readonly stderr: string;
  readonly stdoutTruncated: boolean;
  readonly stderrTruncated: boolean;
  readonly stdoutInvalidUtf8: boolean;
  readonly stderrInvalidUtf8: boolean;
}
export const failureReasons = ["queue-full", "queue-timeout", "deadline", "cancelled",
  "binding-closed", "protocol", "oversize", "transport", "process-failed", "cleanup-unknown"] as const;
export type ProbeFailureReason = typeof failureReasons[number];
export class SearchGitProbeFailure extends Error {
  readonly _tag = "SearchGitProbeFailure";
  constructor(readonly reason: ProbeFailureReason, override readonly cause?: unknown) {
    super(`Workspace Git probe failed (${reason}).`);
  }
}
export const probeFailure = (reason: ProbeFailureReason, cause?: unknown) =>
  new SearchGitProbeFailure(reason, cause);
export const isProbeFailure = (cause: unknown): cause is SearchGitProbeFailure =>
  cause instanceof SearchGitProbeFailure;
export interface TrustedBinding { readonly bootstrap: Bootstrap; readonly token: FinderToken; readonly key: WorkspaceKey }
export type BeginInput = Exclude<SearchGitProbeInput, { kind: "ignoredCandidates" }> |
  { readonly kind: "ignoredCandidates"; readonly repository: string;
    readonly pathCount: number; readonly stdinBytes: number; readonly uploadBytes: number };
interface FrameBase extends Bootstrap { readonly probeVersion: 1; readonly token: FinderToken }
interface CallBase extends FrameBase { readonly owner: ProbeOwner; readonly probeId: number }
export type WorkerFrame =
  | (CallBase & { readonly type: "begin"; readonly input: BeginInput })
  | (CallBase & { readonly type: "upload"; readonly sequence: number; readonly base64: string; readonly last: boolean })
  | (CallBase & { readonly type: "cancel" | "terminalAck" })
  | (CallBase & { readonly type: "downloadCredit"; readonly sequence: number })
  | (FrameBase & { readonly type: "scanBegin"; readonly epoch: number; readonly birthRequestId: number })
  | (FrameBase & { readonly type: "scanEnd"; readonly epoch: number });
// scanGranted acknowledges both begin and end. The client correlates the exact
// outstanding transition; an end acknowledgement grants no new scan authority.
export type ParentFrame =
  | (CallBase & { readonly type: "admitted" })
  | (CallBase & { readonly type: "uploadCredit"; readonly sequence: number })
  | (CallBase & { readonly type: "download"; readonly sequence: number;
      readonly stream: "stdout" | "stderr"; readonly base64: string })
  | (CallBase & { readonly type: "done"; readonly output: Omit<ProbeOutput, "stdout" | "stderr">;
      readonly stdoutBytes: number; readonly stderrBytes: number })
  | (CallBase & { readonly type: "failure"; readonly reason: ProbeFailureReason })
  | (FrameBase & { readonly type: "scanGranted"; readonly epoch: number })
  | (FrameBase & { readonly type: "scanRevoked"; readonly epoch: number; readonly reason: ProbeFailureReason });

const byteString = (v: unknown, limit: number): v is string =>
  typeof v === "string" && !v.includes("\0") && Buffer.byteLength(v) <= limit;
// POSIX relative paths preserve literal backslashes/newlines. Filesystem
// containment is additionally checked by the parent with its retained root.
export const repositoryPath = (v: unknown): v is string =>
  byteString(v, PROBE_LIMITS.pathBytes) && (v === "" || (
    !v.startsWith("/") && !/^[A-Za-z]:/.test(v) &&
    v.split("/").every((part) => part !== "" && part !== "." && part !== ".." && part.toLowerCase() !== ".git")
  ));
export const candidatePath = (v: unknown): v is string =>
  repositoryPath(v) && v !== "";
export function validatePaths(paths: unknown): asserts paths is readonly string[] {
  if (!Array.isArray(paths) || paths.length === 0 || paths.length > PROBE_LIMITS.paths ||
      !paths.every(candidatePath)) throw probeFailure("protocol");
  if (paths.reduce((n, p) => n + Buffer.byteLength(p) + 1, 0) > PROBE_LIMITS.stdinBytes ||
      Buffer.byteLength(JSON.stringify(paths)) > PROBE_LIMITS.uploadBytes)
    throw probeFailure("oversize");
}
export function validateInput(v: unknown): asserts v is SearchGitProbeInput {
  if (!record(v)) throw probeFailure("protocol");
  switch (v.kind) {
    case "ignoredPaths": if (keys(v, ["kind"])) return; break;
    case "repositoryPresence":
      if (keys(v, ["kind", "repository"]) && repositoryPath(v.repository)) return; break;
    case "ignoredCandidates":
      if (keys(v, ["kind", "repository", "paths"]) && repositoryPath(v.repository)) {
        validatePaths(v.paths); return;
      } break;
    case "grep":
      if (keys(v, ["kind", "pattern", "regexMode", "ignoreCase", "onlyMatching"]) &&
          byteString(v.pattern, PROBE_LIMITS.frameBytes) &&
          [v.regexMode, v.ignoreCase, v.onlyMatching].every((x) => typeof x === "boolean")) return;
  }
  throw probeFailure("protocol");
}
export const ownerKey = (v: ProbeOwner) => v.kind === "scan" ? `s:${v.epoch}` : `f:${v.requestId}`;
export const tokenKey = (v: FinderToken) => JSON.stringify([
  v.authorityId, v.slotId, v.slotGeneration, v.keyId, v.incarnation,
]);
export const slotKey = (v: Bootstrap) => JSON.stringify([
  v.version, v.build, v.authorityId, v.slotId, v.slotGeneration, v.nonce,
]);
function validOwner(v: unknown): v is ProbeOwner {
  return record(v) && ((v.kind === "foreground" && keys(v, ["kind", "requestId"]) && counter(v.requestId)) ||
    (v.kind === "scan" && keys(v, ["kind", "epoch"]) && counter(v.epoch)));
}
const baseKeys = ["version", "build", "authorityId", "slotId", "slotGeneration", "nonce", "probeVersion", "token"];
const callKeys = [...baseKeys, "owner", "probeId", "type"];
const nonnegative = (v: unknown): v is number => Number.isSafeInteger(v) && (v as number) >= 0;
export function decodeProbeFrame(value: unknown, boot: Bootstrap, side: "worker"): WorkerFrame;
export function decodeProbeFrame(value: unknown, boot: Bootstrap, side: "parent"): ParentFrame;
export function decodeProbeFrame(value: unknown, boot: Bootstrap, side: "worker" | "parent"): WorkerFrame | ParentFrame {
  if (!record(value) || !sameBootstrap(value as unknown as Bootstrap, boot) ||
      value.probeVersion !== 1 || !isToken(value.token) ||
      value.token.authorityId !== boot.authorityId || value.token.slotId !== boot.slotId ||
      value.token.slotGeneration !== boot.slotGeneration) throw probeFailure("protocol");
  // Validate the exact bootstrap fields without accepting unrelated keys.
  decodeBootstrap(Object.fromEntries(baseKeys.slice(0, 6).map((k) => [k, value[k]])));
  const scan = String(value.type).startsWith("scan");
  if (scan) {
    if (!counter(value.epoch)) throw probeFailure("protocol");
    const names = value.type === "scanBegin" ? ["birthRequestId"] : value.type === "scanRevoked" ? ["reason"] : [];
    if (!keys(value, [...baseKeys, "type", "epoch", ...names]) ||
        (side === "worker" ? !["scanBegin", "scanEnd"].includes(String(value.type)) :
          !["scanGranted", "scanRevoked"].includes(String(value.type))) ||
        (value.type === "scanBegin" && !counter(value.birthRequestId)) ||
        (value.type === "scanRevoked" && !failureReasons.includes(value.reason as ProbeFailureReason)))
      throw probeFailure("protocol");
  } else {
    if (!validOwner(value.owner) || !counter(value.probeId)) throw probeFailure("protocol");
    const fields: Record<string, string[]> = side === "worker" ? {
      begin: ["input"], upload: ["sequence", "base64", "last"], cancel: [],
      downloadCredit: ["sequence"], terminalAck: [],
    } : {
      admitted: [], uploadCredit: ["sequence"], download: ["sequence", "stream", "base64"],
      done: ["output", "stdoutBytes", "stderrBytes"], failure: ["reason"],
    };
    const extra = fields[String(value.type)];
    if (!extra || !keys(value, [...callKeys, ...extra])) throw probeFailure("protocol");
    if (extra.includes("sequence") && !nonnegative(value.sequence)) throw probeFailure("protocol");
    if (extra.includes("base64")) decodeProbeChunk(value.base64);
    if (value.type === "upload" && typeof value.last !== "boolean") throw probeFailure("protocol");
    if (value.type === "download" && value.stream !== "stdout" && value.stream !== "stderr") throw probeFailure("protocol");
    if (value.type === "failure" && !failureReasons.includes(value.reason as ProbeFailureReason)) throw probeFailure("protocol");
    if (value.type === "begin") {
      const input = value.input;
      if (record(input) && input.kind === "ignoredCandidates") {
        if (!keys(input, ["kind", "repository", "pathCount", "stdinBytes", "uploadBytes"]) ||
            !repositoryPath(input.repository) || !counter(input.pathCount) || input.pathCount > PROBE_LIMITS.paths ||
            !counter(input.stdinBytes) || input.stdinBytes > PROBE_LIMITS.stdinBytes ||
            !counter(input.uploadBytes) || input.uploadBytes > PROBE_LIMITS.uploadBytes) throw probeFailure("oversize");
      } else validateInput(input);
    }
    if (value.type === "done") {
      const out = value.output;
      if (!record(out) || !keys(out, ["exitCode", "stdoutTruncated", "stderrTruncated", "stdoutInvalidUtf8", "stderrInvalidUtf8"]) ||
          !nonnegative(out.exitCode) || ![out.stdoutTruncated, out.stderrTruncated, out.stdoutInvalidUtf8, out.stderrInvalidUtf8].every((x) => typeof x === "boolean") ||
          !nonnegative(value.stdoutBytes) || !nonnegative(value.stderrBytes) ||
          value.stdoutBytes > PROBE_LIMITS.ignoreBytes || value.stderrBytes > PROBE_LIMITS.ignoreBytes) throw probeFailure("protocol");
    }
  }
  // Bound descriptors even when decoders are called directly by fixtures.
  const encodedBytes = Buffer.byteLength(JSON.stringify(value)) + 1;
  if (encodedBytes > PROBE_LIMITS.frameBytes) throw probeFailure("oversize");
  if (!scan && value.type === "begin" && record(value.input) && value.input.kind !== "grep" &&
      encodedBytes > PROBE_LIMITS.headerBytes) throw probeFailure("oversize");
  return value as unknown as WorkerFrame | ParentFrame;
}
export function decodeProbeChunk(value: unknown): Buffer {
  if (typeof value !== "string" || value.length > Math.ceil(PROBE_LIMITS.chunkBytes / 3) * 4 ||
      !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(value)) throw probeFailure("protocol");
  const bytes = Buffer.from(value, "base64");
  if (bytes.length === 0 || bytes.length > PROBE_LIMITS.chunkBytes || bytes.toString("base64") !== value) throw probeFailure("protocol");
  return bytes;
}
export async function* probeFrames(stream: NodeStream.Readable) {
  yield* frames(stream, PROBE_LIMITS.frameBytes);
}
/** One ordered, byte-bounded writer per FD4, independent of compute credits. */
export class ProbeWriter {
  private tail: Promise<void> = Promise.resolve();
  private queued = 0;
  constructor(private readonly stream: NodeStream.Writable) {}
  send(frame: WorkerFrame | ParentFrame): Promise<void> {
    let bytes: Buffer;
    try { bytes = encodeFrame(frame, PROBE_LIMITS.frameBytes); }
    catch (cause) { return Promise.reject(cause); }
    if (this.queued + bytes.length > PROBE_LIMITS.writerBytes) return Promise.reject(probeFailure("oversize"));
    this.queued += bytes.length;
    const sent = this.tail.then(() => writeEncodedFrame(this.stream, bytes));
    this.tail = sent.catch(() => {});
    return sent.finally(() => { this.queued -= bytes.length; });
  }
}
