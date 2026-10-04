import { describe, expect, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  OrchestrationV2ThreadProjection,
  OrchestrationV2ThreadProjectionJson,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import {
  cancelsSelfSettlement,
  selfSettlementRun,
  selfSettlementTerminalDisposition,
} from "./SelfSettlement.ts";

const caller = {
  providerSessionId: "session-1",
  providerInstanceId: ProviderInstanceId.make("codex"),
};
const owner = {
  providerSessionId: ProviderSessionId.make("session-1"),
  instanceId: caller.providerInstanceId,
  providerThreadId: ProviderThreadId.make("provider-thread-1"),
};
const runId = RunId.make("run-1");
const threadId = ThreadId.make("thread-1");
const commandId = CommandId.make("self-settle-1");
function fixture(): OrchestrationV2ThreadProjection {
  const now = "2026-10-04T00:00:00.000Z";
  const selection = { instanceId: "codex", model: "test-model" };
  return Schema.decodeUnknownSync(Schema.toCodecJson(OrchestrationV2ThreadProjection))({
    thread: {
      createdBy: "user",
      creationSource: "web",
      id: threadId,
      projectId: "project-1",
      title: "Self settlement",
      providerInstanceId: "codex",
      modelSelection: selection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      activeProviderThreadId: owner.providerThreadId,
      lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
      forkedFrom: null,
      createdAt: now,
      updatedAt: now,
      archivedAt: null,
      deletedAt: null,
      selfSettlement: {
        mcpCredentialId: "credential-1",
        commandId,
        runId,
        providerSessionId: owner.providerSessionId,
        providerInstanceId: owner.instanceId,
      },
    },
    runs: [
      {
        id: runId,
        threadId,
        ordinal: 1,
        providerInstanceId: "codex",
        modelSelection: selection,
        providerThreadId: owner.providerThreadId,
        userMessageId: "message-1",
        rootNodeId: "node-1",
        activeAttemptId: "attempt-1",
        status: "running",
        queuePosition: null,
        requestedAt: now,
        startedAt: now,
        completedAt: null,
        checkpointId: null,
        contextHandoffId: null,
      },
    ],
    updatedAt: now,
    attempts: [
      {
        id: "attempt-1",
        runId,
        attemptOrdinal: 1,
        rootNodeId: "node-1",
        providerInstanceId: "codex",
        providerThreadId: owner.providerThreadId,
        providerTurnId: null,
        reason: "initial",
        status: "running",
        startedAt: now,
        completedAt: null,
      },
    ],
    nodes: [],
    subagents: [],
    providerSessions: [],
    providerThreads: [],
    providerTurns: [],
    runtimeRequests: [],
    messages: [],
    visibleTurnItems: [],
    turnItems: [],
    plans: [],
    checkpointScopes: [],
    checkpoints: [],
    contextHandoffs: [],
    contextTransfers: [],
  });
}

describe("self settlement lifecycle decisions", () => {
  it("accepts only the calling session's active run and leaves that run active", () => {
    const projection = fixture();
    expect(selfSettlementRun(projection, caller, owner)?.id).toBe(runId);
    expect(projection.runs[0]?.status).toBe("running");
    expect(
      selfSettlementRun(projection, { ...caller, providerSessionId: "other" }, owner),
    ).toBeUndefined();
    expect(
      selfSettlementRun(
        projection,
        { ...caller, providerInstanceId: ProviderInstanceId.make("other") },
        owner,
      ),
    ).toBeUndefined();
    expect(selfSettlementRun(projection, caller, null)).toBeUndefined();
    expect(selfSettlementRun({ ...projection, attempts: [] }, caller, owner)).toBeUndefined();
    expect(
      selfSettlementRun(
        {
          ...projection,
          thread: { ...projection.thread, activeProviderThreadId: ProviderThreadId.make("other") },
        },
        caller,
        owner,
      ),
    ).toBeUndefined();
  });
  it("keeps intent while the reply and checkpoint are running and settles only completed", () => {
    const projection = fixture();
    for (const status of ["running", "waiting", "completed"] as const) {
      const current = { ...projection, runs: [{ ...projection.runs[0]!, status }] };
      expect(selfSettlementTerminalDisposition(current, runId)).toBe(
        status === "completed" ? "settle" : "ignore",
      );
    }
  });
  it("cancels failed, interrupted, cancelled and rolled-back runs", () => {
    const projection = fixture();
    for (const status of ["failed", "interrupted", "cancelled", "rolled_back"] as const) {
      expect(
        selfSettlementTerminalDisposition(
          { ...projection, runs: [{ ...projection.runs[0]!, status }] },
          runId,
        ),
      ).toBe("cancel");
    }
  });
  it("blocks queued and held user work while ignoring automatic queued wakeups", () => {
    const projection = fixture();
    const queued = {
      ...projection.runs[0]!,
      id: RunId.make("run-2"),
      ordinal: 2,
      status: "queued" as const,
      queueHeld: true,
    };
    const queuedProjection = { ...projection, runs: [...projection.runs, queued] };
    expect(selfSettlementRun(queuedProjection, caller, owner)).toBeUndefined();
    expect(selfSettlementTerminalDisposition(queuedProjection, runId)).toBe("cancel");
    const notification = Schema.decodeUnknownSync(
      Schema.toCodecJson(OrchestrationV2ThreadProjection),
    )({
      ...Schema.encodeSync(OrchestrationV2ThreadProjectionJson)(queuedProjection),
      messages: [
        {
          createdBy: "system",
          creationSource: "server",
          id: queued.userMessageId,
          threadId,
          runId: queued.id,
          nodeId: null,
          role: "user",
          text: "Wake",
          attachments: [],
          streaming: false,
          notification: { source: { kind: "monitor" }, outcome: "completed", summary: "Wake" },
          createdAt: "2026-10-04T00:00:00.000Z",
          updatedAt: "2026-10-04T00:00:00.000Z",
        },
      ],
    });
    expect(selfSettlementRun(notification, caller, owner)?.id).toBe(runId);
  });
  it("never settles a successor on delayed success of the original run", () => {
    const projection = fixture();
    expect(
      selfSettlementTerminalDisposition(
        {
          ...projection,
          runs: [
            { ...projection.runs[0]!, status: "completed" },
            {
              ...projection.runs[0]!,
              id: RunId.make("successor"),
              ordinal: 2,
              status: "completed",
            },
          ],
        },
        runId,
      ),
    ).toBe("cancel");
    expect(selfSettlementTerminalDisposition(projection, RunId.make("older"))).toBe("ignore");
    expect(
      selfSettlementTerminalDisposition(
        { ...projection, thread: { ...projection.thread, selfSettlement: null } },
        runId,
      ),
    ).toBe("ignore");
  });
  it("invalidates new user work in every delivery mode and explicit reverse actions", () => {
    for (const type of [
      "start_immediately",
      "queue_after_active",
      "defer_start",
      "steer_active",
      "restart_active",
    ] as const) {
      expect(
        cancelsSelfSettlement({
          type: "message.dispatch",
          commandId,
          threadId,
          messageId: MessageId.make("new-message"),
          createdBy: "user",
          creationSource: "web",
          text: "Follow up",
          attachments: [],
          dispatchMode: { type, targetRunId: runId },
        }),
      ).toBe(true);
    }
    for (const type of [
      "thread.unsettle",
      "thread.pin",
      "thread.unpin",
      "thread.unsnooze",
      "thread.archive",
      "thread.unarchive",
    ] as const) {
      expect(cancelsSelfSettlement(type === "thread.unsettle"
        ? { type, commandId, threadId, reason: "user" }
        : { type, commandId, threadId })).toBe(true);
    }
    expect(
      cancelsSelfSettlement({
        type: "thread.metadata.update",
        commandId,
        threadId,
        title: "Retitle",
      }),
    ).toBe(false);
  });
});
