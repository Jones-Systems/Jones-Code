import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import {
  LinkedThreadId,
  LinkedThreadsListInput,
  ThreadHandoffToolInput,
  ThreadLinkToolInput,
} from "./linkedThreads.ts";
const decodeLink = Schema.decodeUnknownSync(ThreadLinkToolInput);
const encodeLink = Schema.encodeSync(ThreadLinkToolInput);
const isLink = Schema.is(ThreadLinkToolInput);

describe("linked thread wire contracts", () => {
  it("retains legacy manual input and the optional handoff mirror basis on the wire", () => {
    const manual = {
      action: "link" as const,
      kind: "continuation" as const,
      from: { environmentId: "from", threadId: "origin" },
      to: { environmentId: "to", threadId: "receiver" },
    };
    expect(decodeLink(manual)).not.toHaveProperty("basis");
    const mirrored = { ...manual, basis: "handoff" as const };
    expect(encodeLink(decodeLink(mirrored))).toEqual(mirrored);
    expect(isLink({ ...manual, basis: "launch" })).toBe(false);
  });
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
