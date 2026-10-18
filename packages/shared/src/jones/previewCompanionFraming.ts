import type { CompanionDown, CompanionUp } from "@t3tools/contracts";

export const CHUNK_BYTES = 256 * 1024;
export const MAX_MESSAGE_BYTES = 32 * 1024 * 1024;
const encoder = new TextEncoder();

export class CompanionFrameError extends Error {
  readonly code: 4413 | 4426;
  constructor(code: 4413 | 4426) {
    super(
      code === 4413 ? "Companion message exceeds transport limits." : "Invalid companion chunks.",
    );
    this.code = code;
  }
}

export function encodeFrames(message: CompanionDown | CompanionUp, id: string): readonly string[] {
  const text = JSON.stringify(message);
  const size = encoder.encode(text).byteLength;
  if (size > MAX_MESSAGE_BYTES) throw new CompanionFrameError(4413);
  if (size <= CHUNK_BYTES) return [text];
  const frames: string[] = [];
  let offset = 0;
  while (offset < text.length) {
    // At most four UTF-8 bytes per Unicode scalar; never split a surrogate pair.
    let end = Math.min(offset + CHUNK_BYTES / 4, text.length);
    const last = text.charCodeAt(end - 1);
    if (end < text.length && last >= 0xd800 && last <= 0xdbff) end--;
    frames.push(
      JSON.stringify({
        type: "chunk",
        id,
        index: frames.length,
        final: end === text.length,
        data: text.slice(offset, end),
      }),
    );
    offset = end;
  }
  return frames;
}

export class CompanionFrameDecoder {
  private pending: { id: string; next: number; bytes: number; parts: string[] } | undefined;

  accept(frame: string): unknown | undefined {
    if (encoder.encode(frame).byteLength > MAX_MESSAGE_BYTES) throw new CompanionFrameError(4413);
    let value: unknown;
    try {
      value = JSON.parse(frame);
    } catch {
      throw new CompanionFrameError(4426);
    }
    if (
      typeof value !== "object" ||
      value === null ||
      !("type" in value) ||
      value.type !== "chunk"
    ) {
      if (this.pending) throw new CompanionFrameError(4426);
      if (encoder.encode(frame).byteLength > CHUNK_BYTES) throw new CompanionFrameError(4413);
      return value;
    }
    if (
      !("id" in value) ||
      typeof value.id !== "string" ||
      value.id.length === 0 ||
      value.id.length > 128 ||
      !("index" in value) ||
      !Number.isSafeInteger(value.index) ||
      !("final" in value) ||
      typeof value.final !== "boolean" ||
      !("data" in value) ||
      typeof value.data !== "string"
    )
      throw new CompanionFrameError(4426);
    const bytes = encoder.encode(value.data).byteLength;
    if (bytes > CHUNK_BYTES) throw new CompanionFrameError(4413);
    const pending = this.pending ?? { id: value.id, next: 0, bytes: 0, parts: [] };
    if (pending.id !== value.id || pending.next !== value.index)
      throw new CompanionFrameError(4426);
    pending.bytes += bytes;
    if (pending.bytes > MAX_MESSAGE_BYTES || pending.next >= 1024)
      throw new CompanionFrameError(4413);
    pending.next++;
    pending.parts.push(value.data);
    this.pending = pending;
    if (!value.final) return undefined;
    this.pending = undefined;
    let decoded: unknown;
    try {
      decoded = JSON.parse(pending.parts.join(""));
    } catch {
      throw new CompanionFrameError(4426);
    }
    if (
      typeof decoded === "object" &&
      decoded !== null &&
      "type" in decoded &&
      decoded.type === "chunk"
    )
      throw new CompanionFrameError(4426);
    return decoded;
  }
}
