// @effect-diagnostics nodeBuiltinImport:off -- Bounded read-only candidate-stack adapter; no Git execution or scratch state.
import * as NodePath from "node:path";
import * as NodeZlib from "node:zlib";
import { openAbsolute, sha256, snapshot } from "./Filesystem.ts";

const limits = {
  input: 1024 * 1024,
  tables: 1000,
  tips: 1000,
  expanded: 32 * 1024 * 1024,
  keys: 32 * 1024 * 1024,
  blocks: 10000,
  records: 100000,
};
type Failure = "bounds" | "timeout" | "malformed" | "native_read";
class ReadFailure extends Error {
  readonly kind: Failure;
  constructor(kind: Failure) {
    super(kind);
    this.kind = kind;
  }
}
function requireFact(fact: unknown): asserts fact {
  if (!fact) throw new ReadFailure("malformed");
}
const u64max = (1n << 64n) - 1n;
const u24 = (b: Buffer, p: number) => b.readUIntBE(p, 3);
const number = (v: bigint) => {
  requireFact(v <= BigInt(Number.MAX_SAFE_INTEGER));
  return Number(v);
};

// Default Z_FINISH rejects incomplete/checksum-failing streams. The public
// bytesWritten counter frames adjacent streams; data events enforce the output
// bound because maxOutputLength alone does not bound streaming inflation.
export function inflateReftableBlock(
  input: Buffer,
  length: number,
  signal: AbortSignal,
): Promise<{ bytes: Buffer; consumed: number }> {
  return new Promise((resolve, reject) => {
    const engine = NodeZlib.createInflate({ maxOutputLength: length });
    const chunks: Buffer[] = [];
    let size = 0;
    const abort = () => engine.destroy(new ReadFailure("timeout"));
    engine.on("data", (chunk: Buffer) => {
      size += chunk.length;
      if (size > length) engine.destroy(new ReadFailure("bounds"));
      else chunks.push(chunk);
    });
    let failure: Error | undefined;
    let ended = false;
    engine.once("error", (error) => {
      failure = error instanceof ReadFailure ? error : new ReadFailure("malformed");
    });
    engine.once("end", () => {
      ended = true;
    });
    // Settle only after the native inflater has closed, also on cancellation.
    engine.once("close", () => {
      signal.removeEventListener("abort", abort);
      if (signal.aborted) reject(new ReadFailure("timeout"));
      else if (failure) reject(failure);
      else if (!ended) reject(new ReadFailure("malformed"));
      else resolve({ bytes: Buffer.concat(chunks), consumed: engine.bytesWritten });
    });
    signal.addEventListener("abort", abort, { once: true });
    if (signal.aborted) abort();
    else engine.end(input);
  });
}

type FileEvidence = { bytes: Buffer; identity: string };
export interface ReftableReaderOptions {
  signal?: AbortSignal;
  remaining?: () => number;
  // Failure seams operate on ordinary valid fixture bytes; they are not Git adapters.
  read?: (path: string, limit: number, check: () => void) => Promise<FileEvidence>;
  identity?: (path: string) => Promise<string>;
  inflate?: typeof inflateReftableBlock;
  limits?: Partial<typeof limits>;
}
export type ReftableObservation =
  | {
      outcome: "complete";
      endpoints: string[];
      manifest: { identity: string; digest: string };
      tables: { name: string; identity: string; digest: string; bytes: number }[];
      inputBytes: number;
      expandedBytes: number;
      keyBytes: number;
      blocks: number;
      records: number;
    }
  | { outcome: "unknown"; errorClass: Failure | "permission_denied" | "not_found" };

async function readFile(path: string, limit: number, check: () => void): Promise<FileEvidence> {
  check();
  const file = await openAbsolute(path);
  try {
    const before = await file.stat({ bigint: true });
    if (!before.isFile() || before.size > BigInt(limit)) throw new ReadFailure("bounds");
    const bytes = Buffer.alloc(Number(before.size) + 1);
    let offset = 0;
    while (offset < bytes.length) {
      check();
      const part = await file.read(bytes, offset, Math.min(65536, bytes.length - offset), null);
      if (!part.bytesRead) break;
      offset += part.bytesRead;
    }
    check();
    if (
      offset !== Number(before.size) ||
      snapshot(before) !== snapshot(await file.stat({ bigint: true }))
    )
      throw new ReadFailure("native_read");
    return { bytes: bytes.subarray(0, offset), identity: snapshot(before) };
  } finally {
    await file.close();
  }
}
async function identity(path: string) {
  const file = await openAbsolute(path);
  try {
    return snapshot(await file.stat({ bigint: true }));
  } finally {
    await file.close();
  }
}

class Cursor {
  readonly bytes: Buffer;
  pos: number;
  readonly end: number;
  constructor(bytes: Buffer, pos: number, end: number) {
    this.bytes = bytes;
    this.pos = pos;
    this.end = end;
  }
  take(length: number) {
    requireFact(Number.isSafeInteger(length) && length >= 0 && length <= this.end - this.pos);
    const value = this.bytes.subarray(this.pos, this.pos + length);
    this.pos += length;
    return value;
  }
  variable() {
    let value = 0n;
    for (let i = 0; i < 10; i++) {
      const byte = this.take(1)[0]!;
      value = (value << 7n) | BigInt(byte & 127);
      requireFact(value <= u64max);
      if (!(byte & 128)) return value;
      value++;
    }
    throw new ReadFailure("malformed");
  }
  string() {
    return this.take(number(this.variable()));
  }
}
function crc32(bytes: Buffer) {
  let crc = 0xffffffff;
  for (const byte of bytes) {
    crc ^= byte;
    for (let bit = 0; bit < 8; bit++) crc = (crc >>> 1) ^ (crc & 1 ? 0xedb88320 : 0);
  }
  return (crc ^ 0xffffffff) >>> 0;
}

// This establishes coverage of one stable active stack, not authenticity or a
// general reftable fsck. Index routing and object validity remain Git-owned.
export async function observeReftable(
  directory: string,
  objectFormat: "sha1" | "sha256",
  options: ReftableReaderOptions = {},
): Promise<ReftableObservation> {
  const started = performance.now();
  const controller = new AbortController();
  // @effect-diagnostics-next-line globalTimers:off -- This native Promise adapter owns and clears its inflater cancellation timer in finally.
  const timer = setTimeout(
    () => controller.abort(),
    Math.max(1, Math.min(10000, options.remaining?.() ?? 10000)),
  );
  const abort = () => controller.abort();
  options.signal?.addEventListener("abort", abort, { once: true });
  const check = () => {
    if (
      options.signal?.aborted ||
      controller.signal.aborted ||
      performance.now() - started >= 10000 ||
      (options.remaining?.() ?? 1) <= 0
    )
      throw new ReadFailure("timeout");
  };
  // Injection can tighten budgets, never increase the production ceilings.
  const bound = Object.fromEntries(
    Object.entries(limits).map(([key, value]) => [
      key,
      Math.min(value, options.limits?.[key as keyof typeof limits] ?? value),
    ]),
  ) as typeof limits;
  const counts = { inputBytes: 0, expandedBytes: 0, keyBytes: 0, blocks: 0, records: 0 };
  const spend = (key: keyof typeof counts, amount: number, ceiling: number) => {
    counts[key] += amount;
    if (counts[key] > ceiling) throw new ReadFailure("bounds");
    check();
  };
  const read = options.read ?? readFile;
  const effective = new Map<string, readonly string[]>();
  try {
    const manifestPath = NodePath.join(directory, "tables.list");
    const manifest = await read(manifestPath, bound.input, check);
    spend("inputBytes", manifest.bytes.length, bound.input);
    const text = manifest.bytes.toString("ascii");
    requireFact(Buffer.from(text, "ascii").equals(manifest.bytes));
    requireFact(!text || text.endsWith("\n"));
    const names = text ? text.slice(0, -1).split("\n") : [];
    if (names.length > bound.tables) throw new ReadFailure("bounds");
    requireFact(new Set(names).size === names.length);
    requireFact(names.every((name) => /^[a-zA-Z0-9][a-zA-Z0-9.-]*\.(?:ref|log)$/.test(name)));
    const tables: Extract<ReftableObservation, { outcome: "complete" }>["tables"] = [];
    for (const name of names) {
      check();
      const file = await read(
        NodePath.join(directory, name),
        bound.input - counts.inputBytes,
        check,
      );
      spend("inputBytes", file.bytes.length, bound.input);
      const bytes = file.bytes;
      requireFact(bytes.length >= 24 + 68 && bytes.subarray(0, 4).toString() === "REFT");
      const version = bytes[4];
      requireFact(version === 1 || version === 2);
      const header = version === 1 ? 24 : 28;
      const footerPos = bytes.length - (header + 44);
      requireFact(footerPos >= header);
      const footer = bytes.subarray(footerPos);
      requireFact(bytes.subarray(0, header).equals(footer.subarray(0, header)));
      requireFact(crc32(footer.subarray(0, -4)) === footer.readUInt32BE(footer.length - 4));
      const hash = version === 1 ? "sha1" : bytes.subarray(24, 28).toString("ascii");
      requireFact(hash === (objectFormat === "sha1" ? "sha1" : "s256"));
      const hashSize = objectFormat === "sha1" ? 20 : 32;
      const min = bytes.readBigUInt64BE(8),
        max = bytes.readBigUInt64BE(16);
      requireFact(min <= max);
      const blockSize = u24(bytes, 5);
      const refIndex = number(footer.readBigUInt64BE(header));
      const objField = footer.readBigUInt64BE(header + 8);
      const objPos = number(objField >> 5n),
        objKeySize = Number(objField & 31n);
      const objIndex = number(footer.readBigUInt64BE(header + 16));
      const logPos = number(footer.readBigUInt64BE(header + 24));
      const logIndex = number(footer.readBigUInt64BE(header + 32));
      const blocks = new Map<
        number,
        { type: string; section: number; last: Buffer; level: number }
      >();
      const first = new Map<string, number>();
      const lastIndex = new Map<number, number>();
      const rootIndex = new Map<number, { level: number; position: number }>();
      const previous = new Map<string, Buffer>();
      let position = 0,
        section = 0;
      while ((position === 0 ? header : position) < footerPos) {
        check();
        const headerOff = position === 0 ? header : 0;
        const physical = position + headerOff;
        requireFact(physical + 4 <= footerPos);
        const type = String.fromCharCode(bytes[physical]!);
        requireFact(["r", "o", "g", "i"].includes(type));
        requireFact(position !== 0 || type === "r" || type === "g");
        const nextSection = type === "r" ? 0 : type === "o" ? 1 : type === "g" ? 2 : section;
        requireFact(nextSection >= section && (type !== "i" || blocks.size > 0));
        if (type !== "i") {
          requireFact(!lastIndex.has(nextSection));
          if (!first.has(type)) first.set(type, position);
        }
        section = nextSection;
        if (type === "i") lastIndex.set(section, position);
        const length = u24(bytes, physical + 1);
        requireFact(length >= headerOff + 6 && length <= 0xffffff);
        spend("blocks", 1, bound.blocks);
        spend("expandedBytes", length, bound.expanded);
        let block: Buffer, physicalEnd: number;
        if (type === "g") {
          const expanded = await (options.inflate ?? inflateReftableBlock)(
            bytes.subarray(physical + 4, footerPos),
            length - headerOff - 4,
            controller.signal,
          );
          check();
          requireFact(expanded.bytes.length === length - headerOff - 4);
          requireFact(
            Number.isSafeInteger(expanded.consumed) &&
              expanded.consumed > 0 &&
              expanded.consumed <= footerPos - physical - 4,
          );
          block = Buffer.concat([bytes.subarray(position, physical + 4), expanded.bytes]);
          physicalEnd = physical + 4 + expanded.consumed;
        } else {
          requireFact(
            position + length <= footerPos && (type === "i" || !blockSize || length <= blockSize),
          );
          block = bytes.subarray(position, position + length);
          physicalEnd = position + length;
        }
        const restartCount = block.readUInt16BE(length - 2);
        const recordEnd = length - 2 - 3 * restartCount;
        requireFact(restartCount > 0 && recordEnd > headerOff + 4);
        const restarts = new Set<number>();
        let priorRestart = -1;
        for (let i = recordEnd; i < length - 2; i += 3) {
          const offset = u24(block, i);
          requireFact(offset > priorRestart && offset >= headerOff + 4 && offset < recordEnd);
          restarts.add(offset);
          priorRestart = offset;
        }
        requireFact(restarts.has(headerOff + 4));
        const cursor = new Cursor(block, headerOff + 4, recordEnd);
        let key = Buffer.alloc(0);
        let level = 0;
        while (cursor.pos < recordEnd) {
          if (counts.records % 256 === 0) {
            await new Promise<void>((resolve) => setImmediate(resolve));
            check();
          }
          spend("records", 1, bound.records);
          const recordPos = cursor.pos;
          const prefix = number(cursor.variable()),
            suffixTag = cursor.variable();
          const tag = Number(suffixTag & 7n),
            suffix = number(suffixTag >> 3n);
          requireFact(prefix <= key.length);
          if (restarts.has(recordPos)) {
            requireFact(prefix === 0);
            restarts.delete(recordPos);
          }
          spend("keyBytes", prefix + suffix, bound.keys);
          const next = Buffer.concat([key.subarray(0, prefix), cursor.take(suffix)]);
          requireFact(next.length > 0 && (!key.length || Buffer.compare(key, next) < 0));
          if (type !== "i") {
            const prev = previous.get(type);
            requireFact(!prev || Buffer.compare(prev, next) < 0);
            previous.set(type, next);
          }
          key = next;
          if (type === "r") {
            requireFact(tag <= 3 && !key.includes(0));
            const delta = cursor.variable();
            requireFact(delta <= max - min);
            if (tag === 1 || tag === 2) cursor.take(hashSize * tag);
            if (tag === 3) cursor.string();
          } else if (type === "g") {
            requireFact(
              tag <= 1 &&
                key.length > 9 &&
                key[key.length - 9] === 0 &&
                !key.subarray(0, -9).includes(0),
            );
            // The reversed u64 is part of the exact key. Historical tombstones
            // need not fall in this table's transaction header interval.
            const updateIndex = u64max - key.readBigUInt64BE(key.length - 8);
            requireFact(updateIndex >= 0n);
            const endpoints: string[] = [];
            if (tag === 1) {
              endpoints.push(
                cursor.take(hashSize).toString("hex"),
                cursor.take(hashSize).toString("hex"),
              );
              cursor.string(); // identity name, never retained
              cursor.string(); // identity email, never retained
              cursor.variable(); // unsigned timestamp
              cursor.take(2).readInt16BE(); // signed timezone, no prose restrictions
              cursor.string(); // message, never retained
            }
            effective.set(key.toString("hex"), endpoints);
          } else if (type === "i") {
            requireFact(tag === 0);
            const target = blocks.get(number(cursor.variable()));
            requireFact(target && target.section === section && key.equals(target.last));
            requireFact(level === 0 || level === target.level + 1);
            level = target.level + 1;
            const levelKey = `i:${section}:${level}`;
            const prior = previous.get(levelKey);
            requireFact(!prior || Buffer.compare(prior, key) < 0);
            previous.set(levelKey, key);
          } else {
            requireFact(key.length === objKeySize && objKeySize >= 2 && objKeySize <= hashSize);
            const count = tag || number(cursor.variable());
            requireFact(count <= cursor.end - cursor.pos);
            let target = 0n;
            for (let i = 0; i < count; i++) {
              if (i % 256 === 0) check();
              const delta = cursor.variable();
              requireFact(i === 0 || delta > 0n);
              target += delta;
              requireFact(target <= u64max && blocks.get(number(target))?.type === "r");
            }
          }
        }
        requireFact(cursor.pos === recordEnd && restarts.size === 0);
        if (type === "i") {
          const root = rootIndex.get(section);
          requireFact(!root || level >= root.level);
          if (!root || level > root.level) rootIndex.set(section, { level, position });
        }
        blocks.set(position, { type, section, last: key, level });
        position = physicalEnd;
        // Padding is optional only at a block boundary; never scan for a header.
        if (position < footerPos && bytes[position] === 0) {
          requireFact(type !== "g" && blockSize > length);
          const end = position + blockSize - length;
          requireFact(
            end <= footerPos && bytes.subarray(position, end).every((byte) => byte === 0),
          );
          position = end;
        }
      }
      requireFact((position || header) === footerPos);
      requireFact((first.get("o") ?? 0) === objPos && (!objPos ? objKeySize === 0 : true));
      requireFact((first.get("g") ?? 0) === logPos);
      requireFact(
        (rootIndex.get(0)?.position ?? 0) === refIndex &&
          (rootIndex.get(1)?.position ?? 0) === objIndex &&
          (rootIndex.get(2)?.position ?? 0) === logIndex,
      );
      tables.push({ name, identity: file.identity, digest: sha256(bytes), bytes: bytes.length });
    }
    // Every path must still bind to the opened file, then the manifest must
    // still name the same ordered stack. No unlisted or sibling file is read.
    for (const table of tables) {
      check();
      if (
        (await (options.identity ?? identity)(NodePath.join(directory, table.name))) !==
        table.identity
      )
        throw new ReadFailure("native_read");
    }
    const fresh = await read(manifestPath, bound.input, check);
    if (fresh.identity !== manifest.identity || !fresh.bytes.equals(manifest.bytes))
      throw new ReadFailure("native_read");
    const endpoints = new Set<string>();
    for (const values of effective.values()) {
      check();
      for (const tip of values) if (!/^0+$/.test(tip)) endpoints.add(tip);
      if (endpoints.size > bound.tips) throw new ReadFailure("bounds");
    }
    check();
    return {
      outcome: "complete",
      endpoints: [...endpoints].sort(),
      manifest: { identity: manifest.identity, digest: sha256(manifest.bytes) },
      tables,
      ...counts,
    };
  } catch (cause) {
    const code = (cause as NodeJS.ErrnoException)?.code;
    return {
      outcome: "unknown",
      errorClass:
        cause instanceof ReadFailure
          ? cause.kind
          : code === "ENOENT"
            ? "not_found"
            : ["EACCES", "EPERM"].includes(code ?? "")
              ? "permission_denied"
              : "native_read",
    };
  } finally {
    clearTimeout(timer);
    options.signal?.removeEventListener("abort", abort);
    controller.abort();
  }
}
