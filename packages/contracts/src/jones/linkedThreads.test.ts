import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { LinkedThreadId, LinkedThreadsListInput, ThreadHandoffToolInput } from "./linkedThreads.ts";

describe("linked thread wire contracts", () => {
  it("rejects invalid link IDs and bounded input overflow", () => {
    expect(Schema.is(LinkedThreadId)("tl1:" + "a".repeat(64))).toBe(true);
    expect(Schema.is(LinkedThreadId)("tl1:" + "g".repeat(64))).toBe(false);
    expect(
      Schema.is(LinkedThreadsListInput)({
        thread: { environmentId: "env", threadId: "thread" },
        limit: 201,
      }),
    ).toBe(false);
    expect(
      Schema.is(ThreadHandoffToolInput)({
        to: { type: "portable" },
        goal: "g",
        scope: "s",
        status: "ok",
        clientRequestId: "key",
        sourceRefs: Array.from({ length: 17 }, () => ({ kind: "file", ref: "a" })),
      }),
    ).toBe(false);
  });
});
