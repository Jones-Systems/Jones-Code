import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import {
  NativeInvocationContext,
  JonesAppIdentity,
  OrganizationThreadMetadata,
  OrganizationThreadMetadataPage,
} from "./organizationMetadata.ts";

const row = {
  threadId: "thread",
  projectId: "project",
  title: "Thread",
  pinnedAt: null,
  pinOrderKey: null,
  activeOrderKey: null,
  snoozedUntil: null,
  settledOverride: null,
  settledAt: null,
  archivedAt: null,
  createdAt: "2026-08-01T00:00:00Z",
  projectionUpdatedAt: "2026-08-01T00:00:00Z",
  latestUserMessageAt: null,
  latestRunId: null,
  activeRunId: null,
  status: "idle",
  latestRunRequestedAt: null,
  latestRunStartedAt: null,
  latestRunCompletedAt: null,
  hasPendingApprovals: false,
  hasPendingUserInput: false,
  hasActionableProposedPlan: false,
};
describe("organization metadata wire contract", () => {
  it("preserves null origin and unavailable generation", () => {
    const decode = Schema.decodeUnknownSync(NativeInvocationContext);
    const value = {
      environmentId: "environment",
      threadId: "thread",
      effectiveBaseDir: "/synthetic",
      loopbackOrigin: null,
      serverVersion: "0.0.45",
      serverGeneration: null,
    };
    expect(decode(value)).toEqual(value);
    expect(() => decode({ ...value, serverGeneration: 1 })).toThrow();
  });
  it("adds explicit identity while retaining legacy invocation responses", () => {
    const legacy = {
      environmentId: "environment",
      threadId: "thread",
      effectiveBaseDir: "/synthetic",
      loopbackOrigin: null,
      serverVersion: "0.0.45",
      serverGeneration: null,
    };
    const identity = {
      productId: "jones-code",
      productName: "Jones Code",
      version: legacy.serverVersion,
      source: null,
    };
    const decode = Schema.decodeUnknownSync(NativeInvocationContext);
    expect(decode(legacy)).toEqual(legacy);
    expect(decode({ ...legacy, appIdentity: identity }).appIdentity).toEqual(identity);
    const decodeIdentity = Schema.decodeUnknownSync(JonesAppIdentity);
    expect(() => decodeIdentity({ ...identity, productId: "t3-code" })).toThrow();
    expect(() =>
      decodeIdentity({
        ...identity,
        source: { repository: "Jones-Systems/Jones-Code", sha: "bad", tree: "b".repeat(40) },
      }),
    ).toThrow();
  });
  it("enforces bounded titles and keeps null activity", () => {
    const decode = Schema.decodeUnknownSync(OrganizationThreadMetadata);
    expect(decode(row)).toEqual(row);
    expect(() => decode({ ...row, title: "x".repeat(513) })).toThrow();
    expect(decode({ ...row, title: "x".repeat(512) }).title).toHaveLength(512);
  });
  it("does not project body or credential fields", () => {
    const decode = Schema.decodeUnknownSync(OrganizationThreadMetadata);
    expect(
      decode({
        ...row,
        latestVisibleMessage: "private body",
        lastError: "private error",
        pendingRuntimeRequest: { payload: "private" },
        authorizationHeader: "private credential",
      }),
    ).toEqual(row);
  });
  it("requires a nonnegative sequence and offset", () => {
    const decode = Schema.decodeUnknownSync(OrganizationThreadMetadataPage);
    const page = {
      environmentId: "environment",
      snapshotSequence: 7,
      observedAt: "2026-08-01T00:00:00Z",
      threads: [row],
      nextOffset: null,
    };
    expect(decode(page)).toEqual(page);
    expect(() => decode({ ...page, snapshotSequence: -1 })).toThrow();
    expect(() => decode({ ...page, nextOffset: -1 })).toThrow();
  });
});
