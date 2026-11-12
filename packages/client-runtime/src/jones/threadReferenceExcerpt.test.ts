import { afterEach, describe, expect, it, vi } from "@effect/vitest";
import { ThreadId } from "@t3tools/contracts";
import { captureThreadReferenceExcerpt } from "./threadReferenceExcerpt.ts";

const threadId = ThreadId.make("source-thread");
const excerpt = {
  status: "captured",
  capturedAt: "2026-10-21T00:00:00Z",
  sourceSnapshotSequence: 8,
  earlierHistory: "none",
  text: "synthetic bounded context",
};
afterEach(() => vi.useRealTimers());
describe("captureThreadReferenceExcerpt", () => {
  it("preserves the source response without reading any other connection", async () => {
    const request = vi.fn(async () => ({ threadId, excerpt }));
    expect(await captureThreadReferenceExcerpt({ threadId, request })).toEqual(excerpt);
    expect(request).toHaveBeenCalledTimes(1);
  });
  it("rejects a different source thread or malformed output", async () => {
    for (const value of [
      { threadId: "other", excerpt },
      { threadId, excerpt: { ...excerpt, text: "x".repeat(16_001) } },
    ]) {
      expect(
        await captureThreadReferenceExcerpt({ threadId, request: async () => value }),
      ).toMatchObject({ status: "unavailable", reason: "invalid_response" });
    }
  });
  it("keeps unsupported and disconnected sources unavailable without fallback", async () => {
    for (const [error, reason] of [
      [new Error("Unknown RPC method"), "unsupported_source_server"],
      [{ _tag: "EnvironmentRpcUnavailableError" }, "source_disconnected"],
    ] as const) {
      const request = vi.fn(async () => {
        throw error;
      });
      expect(await captureThreadReferenceExcerpt({ threadId, request })).toMatchObject({
        status: "unavailable",
        reason,
      });
      expect(request).toHaveBeenCalledTimes(1);
    }
  });
  it("recognizes the old Effect RPC server's unknown request defect", async () => {
    const result = await captureThreadReferenceExcerpt({
      threadId,
      request: async () => {
        throw "Unknown request tag: orchestration.getThreadReferenceExcerpt";
      },
    });
    expect(result).toMatchObject({ status: "unavailable", reason: "unsupported_source_server" });
  });
  it("settles at eight seconds even when the source never responds", async () => {
    vi.useFakeTimers();
    const result = captureThreadReferenceExcerpt({
      threadId,
      request: () => new Promise(() => {}),
    });
    await vi.advanceTimersByTimeAsync(8_000);
    expect(await result).toMatchObject({ status: "unavailable", reason: "timeout" });
    expect(vi.getTimerCount()).toBe(0);
  });
});
