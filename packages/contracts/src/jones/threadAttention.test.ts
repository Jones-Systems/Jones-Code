import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";
import { ExecutionEnvironmentDescriptor } from "../environment.ts";
import { THREAD_CORPUS_CAPABILITY, ThreadCorpusCapability } from "../threadCorpusProtocol.ts";
import corpusFixture from "../../test-fixtures/thread-corpus-capability.json" with { type: "json" };
import { THREAD_ATTENTION_CAPABILITY, ThreadAttention } from "./threadAttention.ts";

const decodeDescriptor = Schema.decodeUnknownSync(ExecutionEnvironmentDescriptor);
const decodeCorpusCapability = Schema.decodeUnknownSync(ThreadCorpusCapability);
const decodeAttention = Schema.decodeUnknownSync(ThreadAttention);

const descriptor = {
  environmentId: "test",
  label: "Test",
  platform: { os: "darwin", arch: "arm64" },
  serverVersion: "0.0.32",
  capabilities: { repositoryIdentity: true },
};

describe("thread attention wire compatibility", () => {
  it("accepts absent attention and adds a sibling without changing exact corpus claims", () => {
    expect(decodeDescriptor(descriptor).capabilities.threadAttention).toBeUndefined();
    const result = decodeDescriptor({
      ...descriptor,
      capabilities: {
        ...descriptor.capabilities,
        threadCorpus: corpusFixture,
        threadAttention: THREAD_ATTENTION_CAPABILITY,
      },
    });
    expect(result.capabilities.threadAttention).toEqual({
      schemaVersion: "t3.thread-attention/v1",
    });
    expect(result.capabilities.threadCorpus).toEqual(corpusFixture);
    expect(THREAD_CORPUS_CAPABILITY).toEqual(corpusFixture);
    expect(() =>
      decodeCorpusCapability({
        ...corpusFixture,
        threadAttention: THREAD_ATTENTION_CAPABILITY,
      }),
    ).toThrow();
  });
  it("does not encode review readiness, bodies, request versions or per-thread cursors", () => {
    const value = {
      ...THREAD_ATTENTION_CAPABILITY,
      threadId: "test",
      snapshotSequence: 42,
      status: "ready",
      pill: null,
      unseenCompletion: false,
      pendingChildKind: null,
      ownRequest: { id: "auth", kind: "auth_refresh", createdAt: "2026-10-18T12:00:00.000Z" },
      coverage: {
        source: "shell",
        childRequests: "active-shell",
        latestVisibleMessage: "unavailable",
        requestVersion: "unavailable",
        requestDetails: "unavailable",
        threadSequence: "unavailable",
        lastVisitedAt: "available",
      },
    };
    expect(decodeAttention(value)).toEqual(value);
    expect(() => decodeAttention({ ...value, status: "ready-for-review" })).toThrow();
  });
});
