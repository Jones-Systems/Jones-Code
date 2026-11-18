import { describe, expect, it } from "@effect/vitest";
import {
  decodeProbeFrame,
  validateInput,
  PROBE_LIMITS,
  ProbeWriter,
} from "./searchProbeProtocol.ts";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- The private FD4 writer fixture observes Node writable drain/backpressure events on its exact bounded fixture stream.
import * as NodeStream from "node:stream";

const boot = {
  version: 1 as const,
  build: "a".repeat(64),
  authorityId: "authority",
  slotId: 0,
  slotGeneration: 1,
  nonce: "nonce",
};
const token = {
  authorityId: "authority",
  slotId: 0,
  slotGeneration: 1,
  keyId: "key",
  incarnation: 1,
};
const frame = {
  ...boot,
  probeVersion: 1,
  token,
  owner: { kind: "foreground", requestId: 1 },
  probeId: 1,
  type: "begin",
  input: { kind: "ignoredPaths" },
};

describe("closed search probe protocol", () => {
  it("binds the complete boot/token/owner and rejects executable surface injection", () => {
    expect(decodeProbeFrame(frame, boot, "worker")).toEqual(frame);
    for (const change of [
      { nonce: "old" },
      { token: { ...token, incarnation: 0 } },
      { owner: { kind: "foreground", requestId: 1, epoch: 1 } },
      { cwd: "/other" },
      { input: { kind: "ignoredPaths", args: ["fetch"] } },
      { input: { kind: "spawn", command: "git" } },
    ])
      expect(() => decodeProbeFrame({ ...frame, ...change }, boot, "worker")).toThrow();
  });

  it("keeps literal POSIX names but rejects traversal and per-probe upload overflow", () => {
    expect(() =>
      validateInput({
        kind: "ignoredCandidates",
        repository: "nested",
        paths: ["newline\nfile", "literal\\backslash", "unicode-λ"],
      }),
    ).not.toThrow();
    for (const repository of ["../x", "/x", "x//y", "x/.git", "C:/outside"])
      expect(() => validateInput({ kind: "repositoryPresence", repository })).toThrow();
    expect(() =>
      validateInput({
        kind: "ignoredCandidates",
        repository: "",
        paths: Array.from({ length: PROBE_LIMITS.paths + 1 }, () => "x"),
      }),
    ).toThrow();
    expect(() =>
      validateInput({
        kind: "ignoredCandidates",
        repository: "",
        paths: ["x".repeat(PROBE_LIMITS.pathBytes + 1)],
      }),
    ).toThrow();
  });

  it("validates canonical base64, stream metadata and encoded frame limits", () => {
    const base = { ...frame, type: "download", sequence: 0, stream: "stdout", base64: "eA==" };
    const { input: _input, ...download } = base;
    expect(decodeProbeFrame(download, boot, "parent")).toEqual(download);
    for (const change of [
      { base64: "eA" },
      { sequence: -1 },
      { stream: "stdin" },
      { base64: "A".repeat(70_000) },
    ])
      expect(() => decodeProbeFrame({ ...download, ...change }, boot, "parent")).toThrow();
  });

  it("refuses bounded writer overflow before enqueueing unbounded output", async () => {
    const pipe = new NodeStream.PassThrough({ highWaterMark: 1 });
    const writer = new ProbeWriter(pipe);
    const writes: Promise<void>[] = [];
    // An undrained pipe holds the first write; queued encoded bytes stay bounded.
    for (let i = 0; i < 8; i++)
      writes.push(
        writer.send({
          ...boot,
          probeVersion: 1,
          token,
          owner: { kind: "foreground", requestId: 1 },
          probeId: 1,
          type: "download",
          sequence: i,
          stream: "stdout",
          base64: Buffer.alloc(PROBE_LIMITS.chunkBytes).toString("base64"),
        }),
      );
    const observed = Promise.allSettled(writes);
    pipe.resume();
    const results = await observed;
    expect(results.some((result) => result.status === "rejected")).toBe(true);
    pipe.destroy();
  });
});
