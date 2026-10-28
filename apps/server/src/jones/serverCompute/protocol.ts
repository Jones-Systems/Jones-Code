// @effect-diagnostics nodeBuiltinImport:off preferSchemaOverJson:off -- Closed validated JSON is framed on a bounded private pipe.
import type { Readable, Writable } from "node:stream";
import { setImmediate as nodeTurn } from "node:timers/promises";
import { isAbsolute } from "node:path";
import type { FinderToken, WorkspaceJobs, WorkspaceKey } from "../serverParallelism/computeContract.ts";
import { computeFailure } from "./failure.ts";

export const LIMITS = Object.freeze({
  requestBytes: 64 * 1024, frameBytes: 1024 * 1024, resultBytes: 16 * 1024 * 1024,
  batchBytes: 256 * 1024, batchEntries: 256, credits: 2, diagnosticsBytes: 64 * 1024,
  // Included in resultBytes: enough for a terminal envelope and the escaped 16-KiB regex error.
  terminalBytes: 128 * 1024,
  queue: 64, perKeyQueue: 8, handles: 256, residents: 4,
  queueMs: 10_000, startupMs: 10_000, jobMs: 30_000,
  idleMs: 15 * 60_000, destroyMs: 2_000, reapMs: 5_000,
});
export const COMMAND = "__jones-server-compute";
export type Operation = keyof WorkspaceJobs | "open" | "close" | "status";
export interface Bootstrap {
  readonly version: 1;
  readonly build: string;
  readonly authorityId: string;
  readonly slotId: number;
  readonly slotGeneration: number;
  readonly nonce: string;
}
export interface ScanStatus { readonly scanning: boolean; readonly warmupComplete: boolean; }
interface Envelope extends Bootstrap {
  readonly requestId: number;
  readonly operation: Operation;
  readonly token: FinderToken;
}
export interface Request extends Envelope {
  readonly input: unknown;
  readonly clock?: { readonly parentSentMs: number };
}
/** Four timestamps exchange epochs without assuming symmetric latency or a shared time origin. */
export interface ReplyTiming {
  readonly parentSentMs: number;
  readonly receivedMs: number;
  readonly executionStartedMs: number;
  readonly executionCompletedMs: number;
  readonly executionOutcome: "ok" | "failed" | "cancelled";
  readonly replyStartedMs: number;
  /** Immediately before terminal encoding; delivery includes that bounded envelope's encoding. */
  readonly handoffMs: number;
}
export interface Reply extends Envelope {
  readonly sequence: number;
  readonly timing?: ReplyTiming;
  readonly body:
    | { readonly type: "chunk"; readonly items: readonly unknown[] }
    | { readonly type: "done"; readonly status?: ScanStatus; readonly truncated?: boolean;
        readonly regexFallbackError?: string }
    | { readonly type: "failure"; readonly reason: "finder-create" | "finder-search" | "finder-refresh" | "cancelled" | "oversize";
        readonly scanTimedOut: boolean };
}
const bootKeys = ["version", "build", "authorityId", "slotId", "slotGeneration", "nonce"];
const envelopeKeys = [...bootKeys, "requestId", "operation", "token"];
const tokenKeys = ["authorityId", "slotId", "slotGeneration", "keyId", "incarnation"];
export function record(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
export function keys(value: Record<string, unknown>, required: readonly string[], optional: readonly string[] = []): boolean {
  return required.every((key) => Object.hasOwn(value, key)) &&
    Object.keys(value).every((key) => required.includes(key) || optional.includes(key));
}
export const counter = (value: unknown): value is number => Number.isSafeInteger(value) && (value as number) > 0;
const monotonicMs = (value: unknown): value is number => typeof value === "number" && Number.isFinite(value) && value >= 0 && value <= Number.MAX_SAFE_INTEGER;
function isReplyTiming(value: unknown): value is ReplyTiming {
  if (!record(value) || !keys(value, ["parentSentMs", "receivedMs", "executionStartedMs", "executionCompletedMs",
    "executionOutcome", "replyStartedMs", "handoffMs"]) ||
    !["ok", "failed", "cancelled"].includes(String(value.executionOutcome))) return false;
  const times = [value.receivedMs, value.executionStartedMs, value.executionCompletedMs, value.replyStartedMs, value.handoffMs];
  return monotonicMs(value.parentSentMs) && times.every(monotonicMs) &&
    times.every((time, i) => i === 0 || (time as number) >= (times[i - 1] as number)) &&
    (value.handoffMs as number) - (value.receivedMs as number) <= LIMITS.jobMs + 1000;
}
export function estimateReplyDelivery(timing: ReplyTiming, parentReceivedMs: number) {
  if (!monotonicMs(parentReceivedMs) || parentReceivedMs < timing.parentSentMs) return undefined;
  const roundTrip = parentReceivedMs - timing.parentSentMs;
  if (roundTrip > LIMITS.jobMs + 1000) return undefined;
  // A declared 1000-ppm relative-rate allowance plus 1 ms resolution allowance per exchange.
  // Out-of-envelope clocks yield unavailable telemetry, never a scheduling/protocol dependency.
  const drift = 1 + Math.max(roundTrip, timing.handoffMs - timing.receivedMs) * .001;
  const offsetLowerMs = timing.parentSentMs - timing.receivedMs - drift;
  const offsetUpperMs = parentReceivedMs - timing.handoffMs + drift;
  if (offsetUpperMs < offsetLowerMs) return undefined;
  const lowerMs = 0, upperMs = Math.max(0, parentReceivedMs - timing.handoffMs - offsetLowerMs);
  return { lowerMs, upperMs, durationMs: upperMs / 2, uncertaintyMs: upperMs / 2, offsetLowerMs, offsetUpperMs };
}
export const identifier = (value: unknown): value is string => typeof value === "string" && /^[a-zA-Z0-9_-]{1,128}$/.test(value);
export const buildId = (value: unknown): value is string => typeof value === "string" && /^[a-f0-9]{64}$/.test(value);
export function decodeBootstrap(value: unknown): Bootstrap {
  if (!record(value) || !keys(value, bootKeys) || value.version !== 1 || !buildId(value.build) ||
      !identifier(value.authorityId) || !identifier(value.nonce) ||
      !Number.isInteger(value.slotId) || (value.slotId as number) < 0 || (value.slotId as number) > 2 || !counter(value.slotGeneration))
    throw computeFailure("protocol");
  return value as unknown as Bootstrap;
}
export function isKey(value: unknown): value is WorkspaceKey {
  return record(value) && keys(value, ["cwd", "variant"]) && typeof value.cwd === "string" &&
    value.cwd.length <= 16_384 && !value.cwd.includes("\0") && isAbsolute(value.cwd) &&
    (value.variant === "paths" || value.variant === "content");
}
export function isToken(value: unknown): value is FinderToken {
  return record(value) && keys(value, tokenKeys) && identifier(value.authorityId) && identifier(value.keyId) &&
    Number.isInteger(value.slotId) && (value.slotId as number) >= 0 && (value.slotId as number) <= 2 &&
    counter(value.slotGeneration) && counter(value.incarnation);
}
export function sameToken(a: FinderToken, b: FinderToken): boolean {
  return tokenKeys.every((key) => a[key as keyof FinderToken] === b[key as keyof FinderToken]);
}
export function sameBootstrap(a: Bootstrap, b: Bootstrap): boolean {
  return bootKeys.every((key) => a[key as keyof Bootstrap] === b[key as keyof Bootstrap]);
}
function envelope(value: unknown, boot: Bootstrap): asserts value is Record<string, unknown> & Envelope {
  if (!record(value) || !sameBootstrap(value as unknown as Bootstrap, boot) || !counter(value.requestId) ||
      !["open", "close", "status", "list", "search", "searchContents", "refresh"].includes(String(value.operation)) ||
      !isToken(value.token) || value.token.authorityId !== boot.authorityId ||
      value.token.slotId !== boot.slotId || value.token.slotGeneration !== boot.slotGeneration)
    throw computeFailure("protocol");
}
export function validInput(operation: Operation, input: unknown): boolean {
  if (operation === "open") return isKey(input);
  // undefined is represented as null on the wire.
  if (operation !== "search" && operation !== "searchContents") return input === null;
  if (!record(input) || typeof input.query !== "string" || !counter(input.limit)) return false;
  if (operation === "search") return keys(input, ["query", "limit"], ["kind", "imageOnly"]) &&
    (input.kind === undefined || input.kind === "file" || input.kind === "directory") &&
    (input.imageOnly === undefined || typeof input.imageOnly === "boolean");
  return keys(input, ["query", "limit", "caseSensitive", "wholeWord", "useRegex"]) &&
    typeof input.caseSensitive === "boolean" && typeof input.wholeWord === "boolean" && typeof input.useRegex === "boolean";
}
export function decodeRequest(value: unknown, boot: Bootstrap): Request {
  envelope(value, boot);
  if (!keys(value, [...envelopeKeys, "input"], ["clock"]) || !validInput(value.operation, value.input) ||
      (value.clock !== undefined && (!record(value.clock) || !keys(value.clock, ["parentSentMs"]) ||
        !monotonicMs(value.clock.parentSentMs)))) throw computeFailure("protocol");
  return value as unknown as Request;
}
export function isStatus(value: unknown): value is ScanStatus {
  return record(value) && keys(value, ["scanning", "warmupComplete"]) &&
    typeof value.scanning === "boolean" && typeof value.warmupComplete === "boolean";
}
function validItem(operation: Operation, item: unknown): boolean {
  if (!record(item)) return false;
  if (operation === "list" || operation === "search") return keys(item, ["path", "kind"]) &&
    typeof item.path === "string" && (item.kind === "file" || item.kind === "directory");
  return operation === "searchContents" && keys(item, ["path", "lineNumber", "lineContent", "matchRanges"]) &&
    typeof item.path === "string" && counter(item.lineNumber) && typeof item.lineContent === "string" &&
    Array.isArray(item.matchRanges) && item.matchRanges.every((range) =>
      record(range) && keys(range, ["start", "end"]) && Number.isSafeInteger(range.start) &&
      Number.isSafeInteger(range.end) && (range.start as number) >= 0 && (range.end as number) >= (range.start as number) &&
      (range.end as number) <= (item.lineContent as string).length);
}
export function decodeReply(value: unknown, boot: Bootstrap): Reply {
  envelope(value, boot);
  if (!keys(value, [...envelopeKeys, "sequence", "body"], ["timing"]) || !Number.isSafeInteger(value.sequence) ||
      (value.sequence as number) < 0 || !record(value.body)) throw computeFailure("protocol");
  if (value.timing !== undefined && (value.body.type === "chunk" || !isReplyTiming(value.timing))) throw computeFailure("protocol");
  const b = value.body;
  if (b.type === "chunk") {
    if (!keys(b, ["type", "items"]) || !Array.isArray(b.items) || b.items.length === 0 || b.items.length > LIMITS.batchEntries ||
        !b.items.every((item) => validItem(value.operation, item))) throw computeFailure("protocol");
  } else if (b.type === "done") {
    if (!keys(b, ["type"], ["status", "truncated", "regexFallbackError"]) ||
        (b.status !== undefined && !isStatus(b.status)) ||
        (b.truncated !== undefined && typeof b.truncated !== "boolean") ||
        (b.regexFallbackError !== undefined && (typeof b.regexFallbackError !== "string" || b.regexFallbackError.length > 16_384)))
      throw computeFailure("protocol");
    const data = ["list", "search", "searchContents"].includes(value.operation);
    if (data !== (typeof b.truncated === "boolean") || (value.operation !== "close" && b.status === undefined) ||
        (value.operation !== "searchContents" && b.regexFallbackError !== undefined)) throw computeFailure("protocol");
  } else if (b.type === "failure") {
    if (!keys(b, ["type", "reason", "scanTimedOut"]) || typeof b.scanTimedOut !== "boolean" ||
        !["finder-create", "finder-search", "finder-refresh", "cancelled", "oversize"].includes(String(b.reason))) throw computeFailure("protocol");
  } else throw computeFailure("protocol");
  return value as unknown as Reply;
}

/** Incremental framing: reject a declared oversize before allocation; never buffer an unbounded pipe. */
export async function* frames(stream: Readable, maxBytes: number, onDecodedBytes?: (bytes: number) => void): AsyncGenerator<unknown> {
  const header = Buffer.alloc(4);
  let headerUsed = 0, payload: Buffer | undefined, used = 0;
  for await (const raw of stream) {
    const input = Buffer.isBuffer(raw) ? raw : Buffer.from(raw);
    let offset = 0;
    while (offset < input.length) {
      if (payload === undefined) {
        const n = Math.min(4 - headerUsed, input.length - offset);
        input.copy(header, headerUsed, offset, offset + n); offset += n; headerUsed += n;
        if (headerUsed < 4) continue;
        const length = header.readUInt32BE();
        if (length === 0 || length > maxBytes) throw computeFailure("oversize");
        payload = Buffer.allocUnsafe(length); used = 0;
      }
      const n = Math.min(payload.length - used, input.length - offset);
      input.copy(payload, used, offset, offset + n); offset += n; used += n;
      if (used === payload.length) {
        const bytes = payload;
        payload = undefined; headerUsed = 0;
        let decoded: unknown;
        try { decoded = JSON.parse(bytes.toString("utf8")); } catch (cause) { throw computeFailure("protocol", cause); }
        onDecodedBytes?.(bytes.length);
        yield decoded;
        // Each frame is <=256 entries / 256 KiB on this protocol. Let real I/O/cancellation run.
        await nodeTurn();
      }
    }
  }
  if (headerUsed !== 0 || payload !== undefined) throw computeFailure("protocol");
}
export function encodeFrame(value: unknown, maxBytes: number): Buffer {
  const payload = Buffer.from(JSON.stringify(value));
  if (payload.length > maxBytes) throw computeFailure("oversize");
  return payload;
}
/** Both peers count payload bytes; chunks leave room for one bounded terminal reply. */
export function replyByteTotal(total: number, bytes: number, terminal: boolean): number {
  const next = total + bytes;
  if ((terminal && bytes > LIMITS.terminalBytes) ||
      next > LIMITS.resultBytes - (terminal ? 0 : LIMITS.terminalBytes)) throw computeFailure("oversize");
  return next;
}
export async function writeEncodedFrame(stream: Writable, payload: Buffer): Promise<void> {
  const header = Buffer.allocUnsafe(4); header.writeUInt32BE(payload.length);
  // One bounded write callback observes stream backpressure and errors, without accumulating listeners.
  await new Promise<void>((resolve, reject) => stream.write(Buffer.concat([header, payload]), (error) => error ? reject(error) : resolve()));
}
export async function writeFrame(stream: Writable, value: unknown, maxBytes: number): Promise<number> {
  const payload = encodeFrame(value, maxBytes);
  await writeEncodedFrame(stream, payload);
  return payload.length;
}

/** NDJSON on FD3 is separately bounded; it never shares data-pipe credit. */
export async function* controls(stream: Readable): AsyncGenerator<Record<string, unknown>> {
  let pending = "";
  for await (const raw of stream) {
    pending += Buffer.from(raw).toString("utf8");
    if (Buffer.byteLength(pending) > 16 * 1024) throw computeFailure("protocol");
    let end: number;
    while ((end = pending.indexOf("\n")) >= 0) {
      const line = pending.slice(0, end); pending = pending.slice(end + 1);
      if (Buffer.byteLength(line) > 4096) throw computeFailure("protocol");
      let value: unknown;
      try { value = JSON.parse(line); } catch { throw computeFailure("protocol"); }
      if (!record(value)) throw computeFailure("protocol");
      yield value;
    }
  }
  if (pending.length) throw computeFailure("protocol");
}
export async function writeControl(stream: Writable, value: unknown): Promise<void> {
  const data = JSON.stringify(value) + "\n";
  if (Buffer.byteLength(data) > 4096) throw computeFailure("oversize");
  await new Promise<void>((resolve, reject) => stream.write(data, (error) => error ? reject(error) : resolve()));
}
