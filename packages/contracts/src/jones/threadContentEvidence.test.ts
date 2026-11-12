import { assert, describe, it } from "@effect/vitest";
import * as Schema from "effect/Schema";
import {
  OrchestratorMcpCapabilitiesResult,
  OrchestratorMcpThreadReadInput,
  OrchestratorMcpThreadTimelineItem,
} from "../orchestratorMcp.ts";
import {
  THREAD_CONTENT_EVIDENCE_CAPABILITY,
  THREAD_CONTENT_EVIDENCE_VERSION,
  THREAD_CONTENT_RENDERER_ID,
  ThreadContentEvidence,
} from "./threadContentEvidence.ts";

const decodeRead = Schema.decodeUnknownSync(OrchestratorMcpThreadReadInput);
const decodeItem = Schema.decodeUnknownSync(OrchestratorMcpThreadTimelineItem);
const decodeEvidence = Schema.decodeUnknownSync(ThreadContentEvidence);
const decodeCapabilities = Schema.decodeUnknownSync(OrchestratorMcpCapabilitiesResult);
const oldPeer = Schema.Struct({ threadId: Schema.String });
const decodeOldPeer = Schema.decodeUnknownSync(oldPeer);
const isEvidence = Schema.is(ThreadContentEvidence);

const optIn = { version: THREAD_CONTENT_EVIDENCE_VERSION };
const digest = "a".repeat(64);
const evidence = {
  ...optIn,
  rendererId: THREAD_CONTENT_RENDERER_ID,
  environmentId: "env",
  threadId: "thread",
  availability: "available",
  contentRevision: digest,
  contentSha256: digest,
  totalUtf16Length: 2,
  startOffset: 0,
  endOffset: 2,
};

describe("thread content evidence wire contract", () => {
  it("preserves the opt-in request through the native read schema", () => {
    const input = {
      threadId: "synthetic-thread",
      contentEvidence: { version: "jones.thread-content-evidence/v1" },
    };
    assert.deepEqual<unknown>(decodeRead(input), input);
  });

  it("keeps legacy requests, clamping and item fixtures unchanged", () => {
    const decode = decodeRead;
    assert.deepEqual<unknown>(decode({ threadId: "thread" }), { threadId: "thread" });
    assert.deepEqual<unknown>(decode({ threadId: "thread", textOffset: 99, afterPosition: 1 }), {
      threadId: "thread",
      textOffset: 99,
      afterPosition: 1,
    });
    assert.equal(decode({ threadId: "thread", maxCharsPerItem: 99_999 }).maxCharsPerItem, 50_000);
    const item = {
      position: 0,
      visibility: "local",
      sourceThreadId: "thread",
      itemId: "item",
      runId: null,
      messageId: null,
      createdBy: null,
      creationSource: null,
      type: "reasoning",
      status: "completed",
      title: null,
      text: "ok",
      textTruncated: false,
      updatedAt: "2026-11-01T00:00:00.000Z",
    };
    assert.deepEqual<unknown>(decodeItem(item), item);
    assert.deepEqual<unknown>(decodeItem({ ...item, contentEvidence: evidence }), {
      ...item,
      contentEvidence: evidence,
    });
  });

  it("rejects invalid versions, digests and incompatible opt-in selectors", () => {
    const decode = decodeRead;
    for (const contentEvidence of [
      { version: "v2" },
      { ...optIn, expectedRevision: "A".repeat(64) },
      { ...optIn, expectedRevision: digest + "\n" },
    ])
      assert.throws(() => decode({ threadId: "thread", itemId: "item", contentEvidence }));
    for (const selection of [
      { contentEvidence: { ...optIn, expectedRevision: digest } },
      { contentEvidence: optIn, textOffset: 0 },
      { contentEvidence: optIn, itemId: "item", textOffset: 1 },
      { contentEvidence: optIn, itemId: "item", afterPosition: 0 },
      { contentEvidence: { ...optIn, expectedRevision: digest }, itemId: "item", textOffset: -1 },
    ])
      assert.throws(() => decode({ threadId: "thread", ...selection }));
    const continuation = {
      threadId: "thread",
      itemId: "item",
      textOffset: 50_000,
      contentEvidence: { ...optIn, expectedRevision: digest },
    };
    assert.deepEqual<unknown>(decode(continuation), continuation);
  });

  it("validates evidence bounds and distinguishes unavailable from empty", () => {
    const decode = decodeEvidence;
    assert.deepEqual<unknown>(decode({ ...evidence, totalUtf16Length: 0, endOffset: 0 }), {
      ...evidence,
      totalUtf16Length: 0,
      endOffset: 0,
    });
    for (const bounds of [
      { startOffset: -1 },
      { startOffset: 3 },
      { endOffset: 3 },
      { totalUtf16Length: 4_000_001 },
      { totalUtf16Length: 50_001, endOffset: 50_001 },
      { contentRevision: "invalid" },
      { rendererId: "other-renderer" },
    ])
      assert.throws(() => decode({ ...evidence, ...bounds }));
    const unavailable = {
      ...optIn,
      rendererId: THREAD_CONTENT_RENDERER_ID,
      environmentId: "env",
      threadId: "thread",
      availability: "unavailable",
      reason: "source_content_null",
    };
    assert.deepEqual<unknown>(decode(unavailable), unavailable);
    assert.throws(() => decode({ ...unavailable, reason: "completed" }));
  });

  it("requires both capability and returned evidence after downgrade", () => {
    const old = {
      parentThreadId: null,
      inheritedProviderInstanceId: null,
      inheritedModel: null,
      runtimeMode: "full-access",
      interactionMode: "default",
      providers: [],
      features: {
        appOwnedSubagents: true,
        asyncPolling: true,
        cancellation: true,
        batchThreadCreation: true,
        threadManagement: true,
        incrementalThreadRead: true,
        scheduledTasks: true,
        maxBatchThreads: 20,
      },
    };
    const decode = decodeCapabilities;
    assert.deepEqual<unknown>(decode(old), old);
    const current = {
      ...old,
      features: { ...old.features, threadContentEvidence: THREAD_CONTENT_EVIDENCE_CAPABILITY },
    };
    assert.deepEqual<unknown>(decode(current), current);
    assert.isUndefined(decode(old).features.threadContentEvidence);
    assert.deepEqual<unknown>(decodeOldPeer({ threadId: "thread", contentEvidence: optIn }), {
      threadId: "thread",
    });
    assert.isFalse(isEvidence(undefined));
  });
});
