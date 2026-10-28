// @effect-diagnostics nodeBuiltinImport:off globalTimers:off -- Real response encoding and credits use bounded synthetic pipes, without a native finder.
import { PassThrough } from "node:stream";
import { expect, it } from "vite-plus/test";
import { decodeReply, frames, LIMITS, type Bootstrap, type Request } from "./protocol.ts";
import { WorkerAllocations, WorkerReplies } from "./worker.ts";

const boot: Bootstrap = { version: 1, build: "a".repeat(64), authorityId: "authority", slotId: 0, slotGeneration: 1, nonce: "nonce" };
const request: Request = { ...boot, requestId: 1, operation: "searchContents",
  input: { query: "needle", limit: 3, caseSensitive: false, wholeWord: false, useRegex: false },
  token: { authorityId: boot.authorityId, slotId: 0, slotGeneration: 1, keyId: "key", incarnation: 1 } };

it("rejects an unwritable frame before waiting on exhausted credits and leaves no sequence hole", async () => {
  const pipe = new PassThrough(), decoded: unknown[] = [];
  const read = (async () => { for await (const frame of frames(pipe, LIMITS.batchBytes)) decoded.push(decodeReply(frame, boot)); })();
  try {
    const replies = new WorkerReplies(request, pipe);
    const item = { path: "a.ts", lineNumber: 1, lineContent: "needle", matchRanges: [] };
    await replies.send({ type: "chunk", items: [item] });
    await replies.send({ type: "chunk", items: [item] });
    // All two credits are outstanding; validation must reject without waiting for an impossible third ack.
    const failure = await replies.send({ type: "chunk", items: [{ ...item, lineContent: "x".repeat(LIMITS.batchBytes) }] })
      .then(() => { throw new Error("oversize frame was accepted"); }, (cause: unknown) => cause);
    expect(failure).toMatchObject({ reason: "oversize" });
    replies.acknowledge();
    await replies.failure(failure, new AbortController().signal);
    replies.acknowledge(); replies.acknowledge();
    await replies.settle();
    expect(() => replies.acknowledge()).toThrow();
    pipe.end(); await read;
    expect(decoded).toMatchObject([
      { sequence: 0, body: { type: "chunk" } }, { sequence: 1, body: { type: "chunk" } },
      { sequence: 2, body: { type: "failure", reason: "oversize" } },
    ]);
  } finally { pipe.destroy(); await read.catch(() => {}); }
});

it("keeps four generation allocation tombstones despite failed creates, close ACKs and lost resident mappings", () => {
  const allocations = new WorkerAllocations();
  const token = (id: number) => ({ ...request.token, keyId: `key-${id}`, incarnation: id + 1 });
  const key = (id: number) => ({ cwd: `/synthetic/${id}`, variant: "paths" as const });
  // The production worker reserves before Scope.make/create and never calls a refund on failure or close.
  allocations.reserve(token(0), key(0));
  expect(() => allocations.reserve({ ...token(0), incarnation: 9 }, key(0))).toThrow();
  expect(() => allocations.reserve(token(1), key(0))).toThrow();
  allocations.reserve(token(1), key(1));
  allocations.reserve(token(2), key(2));
  allocations.reserve(token(3), key(3));
  expect(() => allocations.reserve(token(4), key(4))).toThrow();
  // Only a new child generation owns a fresh allocation ledger.
  const replacement = new WorkerAllocations();
  expect(() => replacement.reserve({ ...token(0), slotGeneration: 2 }, key(0))).not.toThrow();
});
