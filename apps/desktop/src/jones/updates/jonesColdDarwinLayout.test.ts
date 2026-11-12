import { describe, expect, it } from "vite-plus/test";
import {
  coldDarwinLayout as layout,
  coldDarwinTarget,
  coldDarwinReciprocal,
  decodeColdDarwinBsd,
  decodeColdDarwinPrivateUnique,
  decodeColdDarwinSocket,
  decodeColdDarwinVnode,
  decodeColdDarwinFds,
} from "./jonesColdDarwinLayout.ts";

describe("held Darwin SDK decoder", () => {
  it("rejects unknown targets without native loading", () => {
    expect(coldDarwinTarget("darwin", "arm64", "25.3.0")).toBe(true);
    for (const tuple of [
      ["linux", "arm64", "25.3.0"],
      ["darwin", "x64", "25.3.0"],
      ["darwin", "arm64", "25.4.0"],
    ]) {
      expect(coldDarwinTarget(tuple[0]!, tuple[1]!, tuple[2]!)).toBe(false);
    }
    expect(layout.privateUniqueQualified).toBe(false);
  });
  it("decodes unsigned values above 2^53 losslessly and honors byte offsets", () => {
    const storage = new Uint8Array(88);
    const bytes = storage.subarray(16, 72);
    const data = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    data.setBigUint64(16, 0x0020000000000001n, true);
    data.setBigUint64(24, 0xfedcba9876543210n, true);
    data.setInt32(32, 7, true);
    expect(decodeColdDarwinPrivateUnique(bytes, 56)).toEqual({
      uniqueId: 0x0020000000000001n,
      parentUniqueId: 0xfedcba9876543210n,
      pidVersion: 7,
    });
  });
  it("requires exact returned lengths, not buffer capacity", () => {
    for (const [decode, size] of [
      [decodeColdDarwinBsd, 136],
      [decodeColdDarwinSocket, 792],
      [decodeColdDarwinVnode, 176],
      [decodeColdDarwinPrivateUnique, 56],
    ] as const) {
      for (const returned of [-1, 0, size - 1, size + 1, NaN])
        expect(() => decode(new Uint8Array(size), returned)).toThrow();
      expect(() => decode(new Uint8Array(size + 1), size)).toThrow();
    }
  });
  it("requires nonzero reciprocal Unix stream references", () => {
    const socket = (endpoint: bigint, peer: bigint) => {
      const bytes = new Uint8Array(792),
        data = new DataView(bytes.buffer);
      data.setInt32(176, 1, true);
      data.setInt32(184, 1, true);
      data.setInt32(256, 3, true);
      data.setBigUint64(160, endpoint, true);
      data.setBigUint64(264, peer, true);
      return decodeColdDarwinSocket(bytes, bytes.length);
    };
    const left = socket(0xfedcba9876543210n, 0x0020000000000001n);
    const right = socket(left.peer, left.endpoint);
    expect(coldDarwinReciprocal(left, right)).toBe(true);
    expect(coldDarwinReciprocal(socket(0n, 0n), socket(0n, 0n))).toBe(false);
    expect(coldDarwinReciprocal(left, { ...right, streamUnix: false })).toBe(false);
    expect(coldDarwinReciprocal(left, socket(7n, 8n))).toBe(false);
  });
  it("rejects empty, truncated, full, and duplicate descriptor scans", () => {
    const bytes = new Uint8Array(8192),
      data = new DataView(bytes.buffer);
    data.setUint32(0, 4, true);
    data.setUint32(4, 2, true);
    expect(decodeColdDarwinFds(bytes, 8)).toEqual([{ fd: 4, socket: true }]);
    for (const count of [-1, 0, 7, 8192, 8193, NaN])
      expect(() => decodeColdDarwinFds(bytes, count)).toThrow();
    data.setUint32(8, 4, true);
    expect(() => decodeColdDarwinFds(bytes, 16)).toThrow();
    expect(() => decodeColdDarwinFds(new Uint8Array(8), 8)).toThrow();
  });
});
