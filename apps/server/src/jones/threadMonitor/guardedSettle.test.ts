import { describe, expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  EventId,
  GuestMessageId,
  GuestShareId,
  MessageId,
  RunId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type ThreadPullRequestLink,
  type OrchestrationV2Subagent,
  type ThreadGuestShare,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as ProjectionStore from "../../orchestration-v2/ProjectionStore.ts";
import type { RecordedThreadProjection } from "../../orchestration-v2/RecordedTypes.ts";
import { monitorSettlementBlocker, readMonitorSettlementBlocker } from "./guardedSettle.ts";
const now = DateTime.makeUnsafe("2026-10-26T07:00:00Z");
const threadId = ThreadId.make("guard-pure");
const instanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId, model: "gpt-5.4" };
const projection: RecordedThreadProjection = {
  thread: {
    id: threadId,
    projectId: ProjectId.make("p"),
    title: "Guard",
    providerInstanceId: instanceId,
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
    forkedFrom: null,
    createdBy: "user",
    creationSource: "web",
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  },
  runs: [],
  attempts: [],
  nodes: [],
  subagents: [],
  providerSessions: [],
  providerThreads: [],
  providerTurns: [],
  runtimeRequests: [],
  messages: [],
  plans: [],
  turnItems: [],
  checkpointScopes: [],
  checkpoints: [],
  contextHandoffs: [],
  contextTransfers: [],
  visibleTurnItems: [],
  updatedAt: now,
};
const task: OrchestrationV2Subagent = {
  id: NodeId.make("task"),
  threadId,
  runId: null,
  parentNodeId: NodeId.make("parent"),
  origin: "app_owned",
  createdBy: "agent",
  driver: ProviderDriverKind.make("codex"),
  providerInstanceId: instanceId,
  providerThreadId: null,
  childThreadId: ThreadId.make("child"),
  nativeTaskRef: null,
  prompt: "Synthetic task",
  title: null,
  model: null,
  status: "completed",
  result: "Completed",
  startedAt: now,
  completedAt: now,
  updatedAt: now,
};
const guestShare = (status: "pending" | "sent" | "dismissed"): ThreadGuestShare => ({
  id: GuestShareId.make("guard-guest-share"),
  host: "env.t3.test",
  createdAt: DateTime.formatIso(now),
  expiresAt: DateTime.formatIso(DateTime.add(now, { hours: 24 })),
  guestName: "Sam",
  messages: [
    {
      id: GuestMessageId.make("guard-guest-message"),
      text: "Please review this suggestion",
      status,
      submittedAt: DateTime.formatIso(now),
    },
  ],
});

describe("guarded settle completeness policy", () => {
  it("blocks authoritative pending guest messages, but permits resolved and absent guests", () => {
    for (const share of [
      undefined,
      null,
      { ...guestShare("sent"), messages: [] },
      guestShare("sent"),
      guestShare("dismissed"),
    ]) {
      expect(
        monitorSettlementBlocker(
          {
            ...projection,
            thread: { ...projection.thread, guestShare: share },
          },
          DateTime.formatIso(now),
        ),
      ).toBeNull();
    }
    expect(
      monitorSettlementBlocker(
        {
          ...projection,
          thread: { ...projection.thread, guestShare: guestShare("pending") },
        },
        DateTime.formatIso(now),
      ),
    ).toBe("guest");
  });
  it("permits known idle records and rejects active, queued and automatic queued runs uniformly", () => {
    expect(monitorSettlementBlocker(projection, DateTime.formatIso(now))).toBeNull();
    for (const status of ["preparing", "queued", "starting", "running", "waiting"] as const) {
      const run = {
        id: "run",
        threadId,
        ordinal: 1,
        providerInstanceId: instanceId,
        modelSelection,
        userMessageId: MessageId.make("automatic"),
        providerThreadId: null,
        rootNodeId: null,
        activeAttemptId: null,
        status,
        requestedAt: now,
        startedAt: null,
        completedAt: null,
        checkpointId: null,
        contextHandoffId: null,
      };
      expect(
        monitorSettlementBlocker({ ...projection, runs: [{ ...run, id: RunId.make("run") }] }),
      ).toBe("run");
    }
  });
  it("rejects watches even when they do not belong to an active provider run", () => {
    const link: ThreadPullRequestLink = {
      host: "github.com",
      repository: "Jones-Systems/Jones-Code",
      number: 1,
      url: "https://github.com/Jones-Systems/Jones-Code/pull/1",
      source: "manual",
      linkedAt: DateTime.formatIso(now),
      snapshot: null,
      stack: null,
      watch: {
        startedAt: DateTime.formatIso(now),
        headSha: null,
        failedChecks: [],
        passedChecks: [],
        passed: false,
        remarksThrough: DateTime.formatIso(now),
        remarkIds: [],
        conflicting: false,
        wakes: 0,
      },
    };
    expect(
      monitorSettlementBlocker({
        ...projection,
        thread: { ...projection.thread, pullRequests: [link] },
      }),
    ).toBe("background");
  });

  it.effect("does not assume a terminal task proves its child or remote lifetime is safe", () =>
    Effect.gen(function* () {
      yield* Effect.gen(function* () {
        const store = yield* ProjectionStore.ProjectionStoreV2;
        yield* store.apply({
          id: EventId.make("root"),
          type: "thread.created",
          threadId,
          occurredAt: now,
          payload: projection.thread,
        });
        yield* store.apply({
          id: EventId.make("task"),
          type: "subagent.updated",
          threadId,
          occurredAt: now,
          payload: task,
        });
        const child = {
          ...projection.thread,
          id: task.childThreadId!,
          lineage: {
            rootThreadId: threadId,
            parentThreadId: threadId,
            relationshipToParent: "subagent" as const,
          },
        };
        yield* store.apply({
          id: EventId.make("child"),
          type: "thread.created",
          threadId: child.id,
          occurredAt: now,
          payload: child,
        });
        expect(yield* readMonitorSettlementBlocker(store, threadId, DateTime.formatIso(now))).toBe(
          "unavailable",
        );
        yield* store.apply({
          id: EventId.make("child-settled"),
          type: "thread.settled",
          threadId: child.id,
          occurredAt: now,
          payload: { ...child, settledAt: now, settledOverride: "settled" },
        });
        expect(
          yield* readMonitorSettlementBlocker(store, threadId, DateTime.formatIso(now)),
        ).toBeNull();
        for (const status of ["pending", "sent", "dismissed"] as const) {
          yield* store.apply({
            id: EventId.make(`child-guest-${status}`),
            type: "thread.guest-share-updated",
            threadId: child.id,
            occurredAt: now,
            payload: {
              ...child,
              settledAt: now,
              settledOverride: "settled",
              guestShare: guestShare(status),
            },
          });
          expect(
            yield* readMonitorSettlementBlocker(store, threadId, DateTime.formatIso(now)),
          ).toBe(status === "pending" ? "child" : null);
        }
        yield* store.apply({
          id: EventId.make("duplicate-child-history"),
          type: "subagent.updated",
          threadId,
          occurredAt: now,
          payload: { ...task, id: NodeId.make("followup-task") },
        });
        expect(
          yield* readMonitorSettlementBlocker(store, threadId, DateTime.formatIso(now)),
        ).toBeNull();
        yield* store.apply({
          id: EventId.make("task-pending"),
          type: "subagent.updated",
          threadId,
          occurredAt: now,
          payload: { ...task, completionDelivery: { state: "pending", observedByRunId: null } },
        });
        expect(yield* readMonitorSettlementBlocker(store, threadId, DateTime.formatIso(now))).toBe(
          "child",
        );
      }).pipe(Effect.provide(ProjectionStore.layerMemory));
      const remote = {
        ...task,
        childThreadId: null,
        remoteChild: {
          environmentId: EnvironmentId.make("remote"),
          threadId: ThreadId.make("remote-thread"),
          label: "Remote",
        },
      };
      expect(
        monitorSettlementBlocker({ ...projection, subagents: [{ ...remote, status: "running" }] }),
      ).toBe("child");
    }),
  );
});
