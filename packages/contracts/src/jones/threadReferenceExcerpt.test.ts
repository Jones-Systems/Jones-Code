import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import * as Option from "effect/Option";
import { ThreadContextRecord, OrchestrationMessageContext } from "../composerContext.ts";
import { ThreadReferenceExcerpt } from "./threadReferenceExcerpt.ts";

const decodeMessageContext = Schema.decodeUnknownSync(OrchestrationMessageContext);
const decodeExcerpt = Schema.decodeUnknownSync(ThreadReferenceExcerpt);

const record = {
  version: 1,
  kind: "thread",
  contextId: "thread_one",
  label: "Source",
  title: "Source",
  threadId: "one",
  environmentId: "source",
};
const excerpt = {
  status: "captured",
  capturedAt: "2026-10-21T00:00:00Z",
  sourceSnapshotSequence: 123,
  earlierHistory: "omitted",
  text: "Recorded context",
};

describe("optional thread reference excerpts", () => {
  it("decodes old records and preserves captured excerpts through message context JSON", () => {
    const decode = Schema.decodeUnknownSync(ThreadContextRecord);
    expect(decode(record)).toEqual(record);
    const context = decodeMessageContext(
      JSON.parse(JSON.stringify({ version: 1, records: [{ ...record, excerpt }] })),
    );
    expect(context.records).toEqual([{ ...record, excerpt }]);
  });
  it("lets an old struct consumer strip the optional new field", () => {
    const oldStruct = Schema.Struct({
      version: Schema.Literal(1),
      kind: Schema.Literal("thread"),
      contextId: Schema.String,
      label: Schema.String,
      title: Schema.String,
      threadId: Schema.String,
      environmentId: Schema.String,
    });
    expect(Schema.decodeUnknownSync(oldStruct)({ ...record, excerpt })).toEqual(record);
  });
  it.each([
    "source_disconnected",
    "not_found",
    "unsupported_source_server",
    "timeout",
    "invalid_response",
  ])("accepts an explicit %s result without inventing history", (reason) => {
    const unavailable = { status: "unavailable", capturedAt: excerpt.capturedAt, reason };
    expect(decodeExcerpt(unavailable)).toEqual(unavailable);
  });
  it("enforces raw text and metadata bounds", () => {
    const decode = Schema.decodeUnknownOption(ThreadReferenceExcerpt);
    expect(Option.isSome(decode({ ...excerpt, text: "x".repeat(16_000) }))).toBe(true);
    for (const invalid of [
      { ...excerpt, text: "x".repeat(16_001) },
      { ...excerpt, sourceSnapshotSequence: -1 },
      { ...excerpt, sourceSnapshotSequence: 1.5 },
      { ...excerpt, earlierHistory: "complete" },
      { ...excerpt, capturedAt: "x".repeat(65) },
      { status: "unavailable", capturedAt: excerpt.capturedAt, reason: "unknown" },
    ])
      expect(Option.isNone(decode(invalid))).toBe(true);
  });
});
