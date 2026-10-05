import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import {
  ExecutionEnvironmentDescriptor,
  NativeInvocationContext,
  OrganizationThreadMetadataPage,
} from "./environment.ts";

const decodeDescriptor = Schema.decodeUnknownSync(ExecutionEnvironmentDescriptor);

const descriptor = {
  environmentId: "environment-1",
  label: "Local",
  platform: { os: "darwin", arch: "arm64" },
  serverVersion: "0.0.32",
  capabilities: { repositoryIdentity: true },
} as const;

describe("NativeInvocationContext", () => {
  const decode = Schema.decodeUnknownSync(NativeInvocationContext);
  const context = {
    environmentId: "environment-1",
    threadId: "thread-1",
    effectiveBaseDir: "/effective/t3-home",
    loopbackOrigin: "http://127.0.0.1:43123",
    serverVersion: "0.0.32",
    serverGeneration: null,
  };

  it("preserves explicit unavailable origin and generation", () => {
    expect(decode(context)).toEqual(context);
    expect(decode({ ...context, loopbackOrigin: null })).toEqual({
      ...context,
      loopbackOrigin: null,
    });
    expect(() => decode({ ...context, serverGeneration: 1 })).toThrow();
  });

  it("requires all allowlisted fields and rejects extra wire fields under strict decoding", () => {
    const strictDecode = Schema.decodeUnknownSync(NativeInvocationContext, {
      onExcessProperty: "error",
    });
    expect(() => strictDecode({ ...context, providerSessionId: "private-session" })).toThrow();
    for (const key of Object.keys(context)) {
      const missing = { ...context } as Record<string, unknown>;
      delete missing[key];
      expect(() => decode(missing)).toThrow();
    }
  });
});

describe("OrganizationThreadMetadataPage", () => {
  const decode = Schema.decodeUnknownSync(OrganizationThreadMetadataPage, {
    onExcessProperty: "error",
  });
  const metadata = {
    threadId: "thread-1",
    title: "Thread",
    projectId: "project-1",
    pinnedAt: null,
    pinOrderKey: null,
    activeOrderKey: null,
    snoozedUntil: null,
    settledOverride: null,
    settledAt: null,
    archivedAt: null,
    createdAt: "2026-08-01T00:00:00.000Z",
    projectionUpdatedAt: "2026-08-01T01:00:00.000Z",
    latestUserMessageAt: null,
    latestTurn: null,
    session: null,
    hasPendingApprovals: false,
    hasPendingUserInput: false,
    hasActionableProposedPlan: false,
    backgroundLiveness: "unknown",
  };
  const page = {
    environmentId: "environment-1",
    snapshotSequence: 42,
    observedAt: "2026-08-01T02:00:00.000Z",
    threads: [metadata],
    nextOffset: null,
  };

  it("retains unknown and nullable background samples without substituting activity", () => {
    expect(decode(page)).toEqual(page);
    for (const backgroundLiveness of [null, "working", "monitoring"]) {
      const thread = { ...metadata, backgroundLiveness };
      expect(decode({ ...page, threads: [thread] }).threads).toEqual([thread]);
    }
  });

  it("accepts optional V2 facts with strict decoding while retaining historical null turn and session fields", () => {
    const v2Activity = {
      latestRunId: "run-1",
      status: "completed",
      latestRunRequestedAt: metadata.createdAt,
      latestRunStartedAt: null,
      latestRunCompletedAt: null,
      activeRunId: null,
      activityRunStatus: null,
      activityRunStartedAt: null,
    };
    const extended = { ...metadata, v2Activity };
    expect(decode({ ...page, threads: [extended] }).threads).toEqual([extended]);
    expect(decode(page).threads[0]).not.toHaveProperty("v2Activity");
    for (const extra of [
      { lastError: "private" },
      { pendingBackgroundTasks: [] },
      { queuedWorkEmpty: true },
    ]) {
      expect(() =>
        decode({ ...page, threads: [{ ...extended, v2Activity: { ...v2Activity, ...extra } }] }),
      ).toThrow();
    }
    expect(() =>
      decode({
        ...page,
        threads: [{ ...extended, v2Activity: { ...v2Activity, status: "ready" } }],
      }),
    ).toThrow();
  });

  it("rejects private nested fields and requires explicit coverage", () => {
    for (const extra of [
      { messages: [] },
      { planProgress: { step: "private" } },
      { updatedAt: "row-time" },
    ]) {
      expect(() => decode({ ...page, threads: [{ ...metadata, ...extra }] })).toThrow();
    }
    expect(() =>
      decode({
        ...page,
        threads: [
          {
            ...metadata,
            session: {
              status: "running",
              activeTurnId: null,
              updatedAt: metadata.projectionUpdatedAt,
              lastError: "private",
            },
          },
        ],
      }),
    ).toThrow();
    expect(() =>
      decode({
        ...page,
        threads: [
          {
            ...metadata,
            latestTurn: {
              turnId: "turn-1",
              state: "running",
              requestedAt: metadata.createdAt,
              startedAt: null,
              completedAt: null,
              assistantMessageId: "private",
            },
          },
        ],
      }),
    ).toThrow();
    const absent = { ...metadata } as Record<string, unknown>;
    delete absent.backgroundLiveness;
    expect(() => decode({ ...page, threads: [absent] })).toThrow();
  });
});

describe("ExecutionEnvironmentDescriptor", () => {
  it("requires an advertised required-worktree bootstrap capability", () => {
    expect(decodeDescriptor(descriptor).capabilities.requiredWorktreeBootstrap).toBeUndefined();
    expect(
      decodeDescriptor({
        ...descriptor,
        capabilities: { ...descriptor.capabilities, requiredWorktreeBootstrap: true },
      }).capabilities.requiredWorktreeBootstrap,
    ).toBe(true);
  });

  it("treats a missing pull-request capability as unsupported under version skew", () => {
    expect(decodeDescriptor(descriptor).capabilities.pullRequests).toBeUndefined();
  });

  it("preserves an advertised pull-request capability", () => {
    expect(
      decodeDescriptor({
        ...descriptor,
        capabilities: { ...descriptor.capabilities, pullRequests: true },
      }).capabilities.pullRequests,
    ).toBe(true);
  });

  it("treats a missing attachment upload capability as unsupported", () => {
    expect(decodeDescriptor(descriptor).capabilities.attachmentUploads).toBeUndefined();
  });

  it("preserves an advertised attachment upload capability", () => {
    expect(
      decodeDescriptor({
        ...descriptor,
        capabilities: { ...descriptor.capabilities, attachmentUploads: true },
      }).capabilities.attachmentUploads,
    ).toBe(true);
  });

  it("preserves the server's generic attachment upload limit", () => {
    expect(
      decodeDescriptor({
        ...descriptor,
        capabilities: {
          ...descriptor.capabilities,
          fileAttachments: { maxUploadBytes: 50 * 1024 * 1024 },
        },
      }).capabilities.fileAttachments,
    ).toEqual({ maxUploadBytes: 50 * 1024 * 1024 });
  });

  it("treats missing server-resolved command context as unsupported", () => {
    expect(decodeDescriptor(descriptor).capabilities.serverResolvedCommandContext).toBeUndefined();
  });

  it("preserves advertised server-resolved command context", () => {
    expect(
      decodeDescriptor({
        ...descriptor,
        capabilities: {
          ...descriptor.capabilities,
          serverResolvedCommandContext: true,
        },
      }).capabilities.serverResolvedCommandContext,
    ).toBe(true);
  });
});
