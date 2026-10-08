import { describe, expect, it } from "vite-plus/test";
import {
  CompanionFrameDecoder,
  CompanionFrameError,
  encodeFrames,
  CHUNK_BYTES,
  MAX_MESSAGE_BYTES,
} from "./framing.ts";

describe("companion framing", () => {
  it("round-trips large CDP payloads without splitting Unicode scalars", () => {
    const message = {
      type: "browser",
      event: { type: "cdp", threadId: "t", tabId: "p", message: '😀\\"'.repeat(80_000) },
    } as const;
    const frames = encodeFrames(message, "screen");
    expect(frames.length).toBeGreaterThan(1);
    const decoder = new CompanionFrameDecoder();
    for (const frame of frames.slice(0, -1)) {
      expect(new TextEncoder().encode(JSON.parse(frame).data).length).toBeLessThanOrEqual(
        CHUNK_BYTES,
      );
      expect(decoder.accept(frame)).toBeUndefined();
    }
    expect(decoder.accept(frames.at(-1)!)).toEqual(message);
  });
  it.each([
    { name: "missing first chunk", parts: [{ index: 1 }] },
    { name: "duplicate chunk", parts: [{ index: 0 }, { index: 0 }] },
    { name: "interleaved identity", parts: [{ index: 0 }, { index: 1, id: "other" }] },
    { name: "UTF-8 overflow", parts: [{ index: 0, data: "😀".repeat(CHUNK_BYTES / 2) }] },
  ])("rejects $name", ({ parts }) => {
    const decoder = new CompanionFrameDecoder();
    expect(() => {
      for (const part of parts)
        decoder.accept(
          JSON.stringify({ type: "chunk", id: "x", final: false, data: "x", ...part }),
        );
    }).toThrow(CompanionFrameError);
  });
  it("bounds total reassembly and rejects nested chunks", () => {
    const decoder = new CompanionFrameDecoder();
    for (let index = 0; index < MAX_MESSAGE_BYTES / CHUNK_BYTES; index++)
      decoder.accept(
        JSON.stringify({
          type: "chunk",
          id: "x",
          index,
          final: false,
          data: "x".repeat(CHUNK_BYTES),
        }),
      );
    expect(() =>
      decoder.accept(
        JSON.stringify({
          type: "chunk",
          id: "x",
          index: MAX_MESSAGE_BYTES / CHUNK_BYTES,
          final: true,
          data: "x",
        }),
      ),
    ).toThrow(CompanionFrameError);
    expect(() =>
      new CompanionFrameDecoder().accept(
        JSON.stringify({ type: "chunk", id: "x", index: 0, final: true, data: '{"type":"chunk"}' }),
      ),
    ).toThrow(CompanionFrameError);
  });
});
