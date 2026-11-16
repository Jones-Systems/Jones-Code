// @effect-diagnostics nodeBuiltinImport:off globalTimers:off globalDate:off -- Synthetic Node process, pipe and clock fixtures exercise the actual ownership boundary.
import * as NodeStream from "node:stream";
import { expect, it } from "vite-plus/test";
import {
  decodeBootstrap,
  decodeReply,
  decodeRequest,
  encodeFrame,
  estimateReplyDelivery,
  frames,
  LIMITS,
  replyByteTotal,
  writeFrame,
  type Bootstrap,
  type Request,
  type ReplyTiming,
} from "./protocol.ts";
const boot: Bootstrap = {
  version: 1,
  build: "a".repeat(64),
  authorityId: "authority",
  slotId: 0,
  slotGeneration: 1,
  nonce: "nonce",
};
const request: Request = {
  ...boot,
  requestId: 1,
  operation: "list",
  input: null,
  token: {
    authorityId: boot.authorityId,
    slotId: 0,
    slotGeneration: 1,
    keyId: "key",
    incarnation: 1,
  },
};

const timing: ReplyTiming = {
  parentSentMs: 100,
  receivedMs: 10_105,
  executionStartedMs: 10_106,
  executionCompletedMs: 10_126,
  executionOutcome: "ok",
  replyStartedMs: 10_127,
  handoffMs: 10_135,
};

it("bounds a four-timestamp exchange across different epochs and asymmetric latency", () => {
  // Actual parent-child offset is -10,000; outbound=5ms, child=30ms, return=20ms.
  const interval = estimateReplyDelivery(timing, 155)!;
  expect(interval.offsetLowerMs).toBeLessThanOrEqual(-10_000);
  expect(interval.offsetUpperMs).toBeGreaterThanOrEqual(-10_000);
  expect(interval.lowerMs).toBe(0);
  expect(interval.upperMs).toBeGreaterThanOrEqual(20);
  expect(interval.durationMs - interval.uncertaintyMs).toBe(interval.lowerMs);
  expect(interval.durationMs + interval.uncertaintyMs).toBe(interval.upperMs);
  const shifted = Object.fromEntries(
    Object.entries(timing).map(([key, value]) => [
      key,
      typeof value === "number" && key !== "parentSentMs" ? value + 500_000 : value,
    ]),
  ) as unknown as ReplyTiming;
  expect(estimateReplyDelivery(shifted, 155)?.durationMs).toBeCloseTo(interval.durationMs);
  expect(estimateReplyDelivery(timing, 100)).toBeUndefined();
  expect(estimateReplyDelivery(timing, 32_000)).toBeUndefined();
});

it("keeps optional telemetry closed, terminal-only, finite, ordered and bounded", () => {
  const { input: _input, ...envelope } = request;
  const reply = {
    ...envelope,
    sequence: 0,
    body: { type: "done", truncated: false, status: { scanning: false, warmupComplete: true } },
    timing,
  };
  expect(decodeReply(reply, boot).timing).toEqual(timing);
  expect(decodeRequest({ ...request, clock: { parentSentMs: 100 } }, boot).clock).toEqual({
    parentSentMs: 100,
  });
  for (const clock of [
    { parentSentMs: -1 },
    { parentSentMs: NaN },
    { parentSentMs: Infinity },
    { parentSentMs: 1, pid: 1 },
  ])
    expect(() => decodeRequest({ ...request, clock }, boot)).toThrow();
  for (const malformed of [
    { ...timing, receivedMs: NaN },
    { ...timing, handoffMs: Infinity },
    { ...timing, executionCompletedMs: 10_104 },
    { ...timing, handoffMs: 50_000 },
    { ...timing, executionOutcome: "timeout" },
    { ...timing, path: "/private" },
  ])
    expect(() => decodeReply({ ...reply, timing: malformed }, boot)).toThrow();
  expect(() =>
    decodeReply({ ...reply, body: { type: "chunk", items: [{ path: "a", kind: "file" }] } }, boot),
  ).toThrow();
  expect(
    decodeReply(
      { ...reply, body: { type: "failure", reason: "finder-search", scanTimedOut: true } },
      boot,
    ).timing,
  ).toEqual(timing);
  const encoded = encodeFrame(reply, LIMITS.batchBytes);
  expect(
    replyByteTotal(LIMITS.resultBytes - LIMITS.terminalBytes, encoded.length, true),
  ).toBeLessThanOrEqual(LIMITS.resultBytes);
});

it("accepts only closed versioned bootstrap and operation shapes", () => {
  expect(decodeBootstrap(boot)).toEqual(boot);
  expect(() => decodeBootstrap({ ...boot, module: "/arbitrary" })).toThrow();
  expect(() => decodeBootstrap({ ...boot, slotGeneration: Number.MAX_SAFE_INTEGER + 1 })).toThrow();
  expect(() => decodeRequest({ ...request, operation: "eval" }, boot)).toThrow();
  expect(() =>
    decodeRequest({ ...request, token: { ...request.token, slotGeneration: 2 } }, boot),
  ).toThrow();
  expect(() =>
    decodeRequest(
      { ...request, operation: "search", input: { query: "x", limit: 2, extra: true } },
      boot,
    ),
  ).toThrow();
  expect(decodeRequest(request, boot)).toEqual(request);
});

it("strictly validates chunk batches and successful completion metadata", () => {
  const { input: _input, ...envelope } = request;
  expect(
    decodeReply(
      {
        ...envelope,
        sequence: 0,
        body: { type: "chunk", items: [{ path: "src/a.ts", kind: "file" }] },
      },
      boot,
    ).sequence,
  ).toBe(0);
  expect(() =>
    decodeReply(
      {
        ...envelope,
        sequence: 0,
        body: {
          type: "chunk",
          items: Array.from({ length: 257 }, () => ({ path: "x", kind: "file" })),
        },
      },
      boot,
    ),
  ).toThrow();
  expect(() => decodeReply({ ...envelope, sequence: 0, body: { type: "done" } }, boot)).toThrow();
  expect(() =>
    decodeReply(
      {
        ...envelope,
        sequence: 0,
        body: {
          type: "done",
          truncated: false,
          status: { scanning: "unknown", warmupComplete: true },
        },
      },
      boot,
    ),
  ).toThrow();
});

it("decodes fragmented length headers/payloads without joining job results", async () => {
  const pipe = new NodeStream.PassThrough(),
    bytes: Buffer[] = [];
  pipe.on("data", (data: Buffer) => bytes.push(data));
  await writeFrame(pipe, request, LIMITS.requestBytes);
  const encoded = Buffer.concat(bytes);
  const parts = Array.from({ length: encoded.length }, (_, i) => encoded.subarray(i, i + 1));
  const values = [];
  for await (const value of frames(NodeStream.Readable.from(parts), LIMITS.requestBytes))
    values.push(value);
  expect(values).toEqual([request]);
  pipe.destroy();
});

it("rejects oversize headers before payload allocation and rejects incomplete/invalid JSON frames", async () => {
  const header = Buffer.alloc(4);
  header.writeUInt32BE(LIMITS.requestBytes + 1);
  const collect = async (bytes: Buffer) => {
    for await (const _value of frames(NodeStream.Readable.from([bytes]), LIMITS.requestBytes)) {
    }
  };
  await expect(collect(header)).rejects.toMatchObject({ reason: "oversize" });
  header.writeUInt32BE(2);
  await expect(collect(Buffer.concat([header, Buffer.from("{")]))).rejects.toMatchObject({
    reason: "protocol",
  });
  await expect(collect(Buffer.concat([header, Buffer.from("xx")]))).rejects.toMatchObject({
    reason: "protocol",
  });
});

it("shares an exact aggregate boundary and reserves bounded terminal space for either outcome", () => {
  const dataBytes = LIMITS.resultBytes - LIMITS.terminalBytes;
  expect(replyByteTotal(dataBytes - LIMITS.batchBytes, LIMITS.batchBytes, false)).toBe(dataBytes);
  expect(() => replyByteTotal(dataBytes, 1, false)).toThrow();
  expect(replyByteTotal(dataBytes, LIMITS.terminalBytes, true)).toBe(LIMITS.resultBytes);
  expect(() => replyByteTotal(dataBytes, LIMITS.terminalBytes + 1, true)).toThrow();
  expect(() => replyByteTotal(LIMITS.resultBytes, 1, true)).toThrow();
  const { input: _input, ...envelope } = request;
  // Exercise the largest escaped terminal metadata, with maximum permitted envelope fields.
  const terminal = {
    ...envelope,
    authorityId: "a".repeat(128),
    nonce: "n".repeat(128),
    requestId: Number.MAX_SAFE_INTEGER,
    operation: "searchContents",
    token: {
      ...request.token,
      authorityId: "a".repeat(128),
      keyId: "k".repeat(128),
      incarnation: Number.MAX_SAFE_INTEGER,
    },
    sequence: Number.MAX_SAFE_INTEGER,
    body: {
      type: "done",
      truncated: false,
      status: { scanning: false, warmupComplete: true },
      regexFallbackError: "\u0000".repeat(16_384),
    },
  };
  const encoded = encodeFrame(terminal, LIMITS.batchBytes);
  expect(encoded.length).toBeLessThanOrEqual(LIMITS.terminalBytes);
  expect(replyByteTotal(dataBytes, encoded.length, true)).toBeLessThanOrEqual(LIMITS.resultBytes);
});
