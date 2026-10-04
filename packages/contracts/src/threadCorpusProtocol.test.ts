import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import capabilityFixture from "../test-fixtures/thread-corpus-capability.json" with { type: "json" };
import { ExecutionEnvironmentDescriptor } from "./environment.ts";
import {
  OrchestrationV2ShellSnapshot,
  OrchestrationV2ThreadDetailSnapshot,
  OrchestrationV2ThreadBoundedSnapshot,
  OrchestrationV2ThreadHistoryPage,
} from "./orchestrationV2.ts";
import { QUEUE_DISPATCH_CAPABILITY } from "./queueProtocol.ts";
import { THREAD_CORPUS_CAPABILITY, ThreadCorpusCapability } from "./threadCorpusProtocol.ts";

const decodeCapability = Schema.decodeUnknownSync(ThreadCorpusCapability);
const timestamp = "2026-10-02T12:00:00.000Z";
const project = {
  id: "project-1",
  title: "Corpus fixture",
  workspaceRoot: "/workspace/project",
  repositoryIdentity: {
    canonicalKey: "github.com/example/project",
    locator: {
      source: "git-remote",
      remoteName: "origin",
      remoteUrl: "https://github.com/example/project.git",
    },
  },
  defaultModelSelection: null,
  scripts: [],
  createdAt: timestamp,
  updatedAt: timestamp,
};
const threadShell = {
  id: "thread-1",
  projectId: project.id,
  title: "Corpus fixture",
  modelSelection: { instanceId: "codex", model: "test-model" },
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: "work/corpus",
  worktreePath: "/workspace/worktrees/corpus",
  createdBy: "user",
  creationSource: "web",
  providerInstanceId: "codex",
  lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: "thread-1" },
  forkedFrom: null,
  activeProviderThreadId: null,
  latestRunId: null,
  activeRunId: null,
  status: "idle",
  pendingRuntimeRequest: null,
  latestVisibleMessage: null,
  itemCount: 1,
  visibleItemCount: 1,
  settledOverride: "settled",
  deletedAt: null,
  createdAt: timestamp,
  updatedAt: timestamp,
  archivedAt: null,
  settledAt: timestamp,
  latestUserMessageAt: timestamp,
  hasActionableProposedPlan: false,
};
const message = {
  id: "message-1",
  role: "user",
  text: "A retained prompt",
  threadId: "thread-1",
  runId: "run-1",
  nodeId: null,
  createdBy: "user",
  creationSource: "web",
  attachments: [],
  streaming: false,
  createdAt: timestamp,
  updatedAt: timestamp,
};
const detail = {
  snapshotSequence: 42,
  projection: {
    thread: threadShell,
    messages: [message],
    runs: [],
    attempts: [],
    nodes: [],
    subagents: [],
    providerSessions: [],
    providerThreads: [],
    providerTurns: [],
    runtimeRequests: [],
    plans: [],
    turnItems: [],
    checkpointScopes: [],
    checkpoints: [],
    contextHandoffs: [],
    contextTransfers: [],
    visibleTurnItems: [],
    updatedAt: timestamp,
  },
};

describe("thread corpus capability", () => {
  it("round-trips the five-field fixture and shares the existing auth contract", () => {
    const decoded = decodeCapability(capabilityFixture);
    expect(Schema.encodeSync(ThreadCorpusCapability)(decoded)).toEqual(capabilityFixture);
    expect(THREAD_CORPUS_CAPABILITY).toEqual(capabilityFixture);
    expect(decoded.authSession).toBe(QUEUE_DISPATCH_CAPABILITY.authSession);
    expect(Object.keys(decoded)).toHaveLength(5);
  });

  it("rejects the legacy V1 wire contract", () => {
    expect(() =>
      decodeCapability({
        ...capabilityFixture,
        schemaVersion: "t3.thread-corpus-capability/v1",
        shellSnapshot: "t3.thread-corpus-shell/v1",
        threadDetailPagination: "t3.thread-corpus-pagination/v1",
      }),
    ).toThrow();
  });

  it("rejects omitted, unsupported and extra capability claims", () => {
    for (const field of Object.keys(capabilityFixture)) {
      const missing: Record<string, unknown> = { ...capabilityFixture };
      delete missing[field];
      expect(() => decodeCapability(missing)).toThrow();
      expect(() => decodeCapability({ ...capabilityFixture, [field]: "unsupported/v2" })).toThrow();
    }
    for (const extra of [{ dispatchGuard: true }, { qualifiedQuota: true }, { future: true }]) {
      expect(() => decodeCapability({ ...capabilityFixture, ...extra })).toThrow();
    }
    for (const malformed of [null, [], "t3.thread-corpus-capability/v1"]) {
      expect(() => decodeCapability(malformed)).toThrow();
    }
  });

  it("preserves the advertisement across build versions and leaves old descriptors valid", () => {
    const decode = Schema.decodeUnknownSync(ExecutionEnvironmentDescriptor);
    const descriptor = {
      environmentId: "environment-1",
      label: "Corpus fixture",
      platform: { os: "linux", arch: "x64" },
      capabilities: { repositoryIdentity: true, threadCorpus: capabilityFixture },
    };
    for (const serverVersion of ["0.0.44-preview.old", "99.0.0-future-build"]) {
      expect(decode({ ...descriptor, serverVersion }).capabilities.threadCorpus).toEqual(
        capabilityFixture,
      );
    }
    expect(
      decode({ ...descriptor, serverVersion: "0.0.44", capabilities: {} }).capabilities
        .threadCorpus,
    ).toBeUndefined();
  });

  it("parses native shell placement, nullable identity, lifecycle and activity sequence", () => {
    const decode = Schema.decodeUnknownSync(Schema.toCodecJson(OrchestrationV2ShellSnapshot));
    const snapshot = decode({
      schemaVersion: 1,
      snapshotSequence: 42,
      archivedThreads: [],
      projects: [project, { ...project, id: "project-2", repositoryIdentity: null }],
      threads: [threadShell, { ...threadShell, id: "thread-2", branch: null, worktreePath: null }],
    });
    expect(snapshot.snapshotSequence).toBe(42);
    expect(snapshot.projects[0]?.workspaceRoot).toBe(project.workspaceRoot);
    expect(snapshot.projects[0]?.repositoryIdentity).toEqual(project.repositoryIdentity);
    expect(snapshot.projects[1]?.repositoryIdentity).toBeNull();
    expect(snapshot.threads[0]?.worktreePath).toBe(threadShell.worktreePath);
    expect(snapshot.threads[1]?.worktreePath).toBeNull();
    expect(DateTime.formatIso(snapshot.threads[0]!.settledAt!)).toBe(timestamp);
    expect(snapshot.threads[0]?.archivedAt).toBeNull();
    expect(DateTime.formatIso(snapshot.threads[0]!.latestUserMessageAt!)).toBe(timestamp);
    expect(DateTime.formatIso(snapshot.threads[0]!.updatedAt!)).toBe(timestamp);
    const encoded = Schema.encodeSync(Schema.toCodecJson(OrchestrationV2ShellSnapshot))(snapshot);
    expect(() => decode({ ...encoded, snapshotSequence: -1 })).toThrow();
    expect(() =>
      decode({ ...encoded, projects: [{ ...project, workspaceRoot: null }] }),
    ).toThrow();
  });

  it("preserves bounded snapshots and opaque chronological history page cursors", () => {
    const decodeDetail = Schema.decodeUnknownSync(Schema.toCodecJson(OrchestrationV2ThreadDetailSnapshot));
    expect(decodeDetail(detail).historyCursor).toBeUndefined();
    const decodeBounded = Schema.decodeUnknownSync(Schema.toCodecJson(OrchestrationV2ThreadBoundedSnapshot));
    const bounded = {
      ...detail,
      historyCursor: "opaque-history-cursor",
      hasMoreHistory: true,
      latestLocalTurnOrdinal: 40,
    };
    expect(decodeBounded(bounded).historyCursor).toBe(bounded.historyCursor);
    expect(decodeBounded(bounded).latestLocalTurnOrdinal).toBe(40);
    expect(() => decodeBounded({ ...bounded, historyCursor: "" })).toThrow();
    expect(() => decodeBounded({ ...bounded, latestLocalTurnOrdinal: -1 })).toThrow();
    const decodePage = Schema.decodeUnknownSync(Schema.toCodecJson(OrchestrationV2ThreadHistoryPage));
    const page = {
      snapshotSequence: 42,
      items: [],
      nextCursor: "opaque-earlier-cursor",
      hasMoreHistory: true,
    };
    expect(decodePage(page)).toEqual(page);
    const oldest = { ...page, nextCursor: null, hasMoreHistory: false };
    expect(decodePage(oldest)).toEqual(oldest);
    expect(() => decodePage({ ...page, nextCursor: 123 })).toThrow();
    expect(() => decodePage({ ...page, nextCursor: "" })).toThrow();
    expect(() => decodePage({ ...page, snapshotSequence: -1 })).toThrow();
  });

  it("preserves message text beyond consumer budgets without inventing native truncation", () => {
    const text = "x".repeat(24001);
    const decoded = Schema.decodeUnknownSync(Schema.toCodecJson(OrchestrationV2ThreadDetailSnapshot))({
      ...detail,
      projection: { ...detail.projection, messages: [{ ...message, text }] },
    });
    expect(decoded.projection.messages[0]?.text).toBe(text);
    expect(decoded.projection.messages[0]?.runId).toBe(message.runId);
    expect(decoded.projection.messages[0]?.streaming).toBe(false);
  });
});
