import { describe, expect, it } from "vite-plus/test";
import {
  CommandId,
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderInstanceId,
  ProviderThreadId,
  RunAttemptId,
  RunId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2Run,
  type OrchestrationV2StoredEvent,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { classifyAutomaticQueueHolds } from "./QueueHoldProvenance.ts";

const now = DateTime.makeUnsafe(0);
const threadId = ThreadId.make("provenance");
const instanceId = ProviderInstanceId.make("codex");
const thread: OrchestrationV2AppThread = {
  id: threadId,
  projectId: ProjectId.make("project"),
  title: "Synthetic provenance",
  providerInstanceId: instanceId,
  modelSelection: { instanceId, model: "synthetic" },
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  activeProviderThreadId: null,
  lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
  forkedFrom: null,
  createdAt: now,
  updatedAt: now,
  archivedAt: null,
  deletedAt: null,
  settledOverride: null,
  settledAt: null,
  lastVisitedAt: null,
  createdBy: "user",
  creationSource: "web",
};
const run = (
  id: string,
  ordinal: number,
  status: OrchestrationV2Run["status"],
): OrchestrationV2Run => ({
  id: RunId.make(id),
  threadId,
  ordinal,
  providerInstanceId: instanceId,
  modelSelection: thread.modelSelection,
  providerThreadId: ProviderThreadId.make("provider"),
  userMessageId: MessageId.make(id),
  rootNodeId: NodeId.make(id),
  activeAttemptId: RunAttemptId.make(id),
  status,
  queuePosition: status === "queued" ? ordinal : null,
  requestedAt: now,
  startedAt: status === "queued" ? null : now,
  completedAt: status === "completed" || status === "failed" ? now : null,
  checkpointId: null,
  contextHandoffId: null,
});
const stored = (
  sequence: number,
  event: OrchestrationV2DomainEvent,
  commandId: CommandId | null = null,
): OrchestrationV2StoredEvent => ({ sequence: sequence * 10, commandId, event });
const update = (
  sequence: number,
  payload: OrchestrationV2Run,
  type: "run.created" | "run.updated" = "run.updated",
  commandId: CommandId | null = null,
) =>
  stored(
    sequence,
    {
      id: EventId.make(`event:${sequence}`),
      type,
      threadId,
      runId: payload.id,
      occurredAt: now,
      payload,
    },
    commandId,
  );
function fixture(preparation = false) {
  const source = {
    ...run("source", 1, "running"),
    ...(preparation
      ? { workspacePreparation: { type: "worktree" as const, baseRef: "main" } }
      : {}),
  };
  const queued = run("queued", 2, "queued");
  const held = { ...queued, queueHeld: true };
  const failed = { ...source, status: "failed" as const, completedAt: now };
  const recovery = run("recovery", 3, "completed");
  const queuedCommand = CommandId.make("queue");
  const events = [
    stored(1, {
      id: EventId.make("create"),
      type: "thread.created",
      threadId,
      occurredAt: now,
      payload: thread,
    }),
    update(2, source, "run.created"),
    update(3, queued, "run.created", queuedCommand),
    stored(4, {
      id: EventId.make("error"),
      type: "turn-item.updated",
      threadId,
      runId: source.id,
      occurredAt: now,
      payload: {
        id: TurnItemId.make("error"),
        type: "error",
        threadId,
        runId: source.id,
        nodeId: source.rootNodeId!,
        providerThreadId: source.providerThreadId,
        providerTurnId: null,
        nativeItemRef: null,
        parentItemId: null,
        ordinal: 1,
        status: "failed",
        title: "Synthetic failure",
        startedAt: now,
        completedAt: now,
        updatedAt: now,
        failure: {
          class: preparation ? "validation_error" : "provider_error",
          message: "Synthetic failure",
          code: preparation ? "workspace_preparation_failed" : "provider_failed",
          retryable: false,
        },
      },
    }),
    update(5, failed),
    update(6, held),
    update(7, { ...recovery, status: "starting", completedAt: null }, "run.created"),
    update(8, recovery),
  ];
  const receipts = [
    { command_id: queuedCommand, command_type: "message.dispatch", result_sequence: 30 },
  ];
  return { source, failed, queued, held, recovery, events, receipts };
}
const eligible = (
  f: ReturnType<typeof fixture>,
  events = f.events,
  receipts = f.receipts,
  completion = events.at(-1)!,
) => [
  ...classifyAutomaticQueueHolds({
    events,
    receipts,
    candidates: [f.held],
    recovery: { type: "completion", stored: completion, current: f.recovery },
  }),
];

describe("automatic queue hold recorded provenance", () => {
  it("proves automatic recovery with equal timestamps using exact execution and event sequence", () => {
    const f = fixture();
    expect(eligible(f)).toEqual([f.queued.id]);
  });
  it("retains a failure-held row acquired by a later accepted Stop without a new hold event", () => {
    const f = fixture();
    expect(
      eligible(f, f.events, [
        ...f.receipts,
        { command_id: CommandId.make("stop"), command_type: "thread.stop", result_sequence: 60 },
      ]),
    ).toEqual([]);
  });
  it("a repeated automatic true write cannot replace an earlier Stop origin", () => {
    const f = fixture();
    const events = f.events.map((event) =>
      event.sequence === 60 ? { ...event, commandId: CommandId.make("stop") } : event,
    );
    events.splice(6, 0, update(6.5, f.held));
    expect(
      eligible(f, events, [
        ...f.receipts,
        { command_id: CommandId.make("stop"), command_type: "thread.stop", result_sequence: 60 },
      ]),
    ).toEqual([]);
  });
  it("retains unexplained holds even when a later failure repeats true", () => {
    const f = fixture();
    const events = f.events.map((event) =>
      event.sequence === 30 ? update(3, f.held, "run.created", CommandId.make("queue")) : event,
    );
    expect(eligible(f, events)).toEqual([]);
  });
  it("a commandless hold and nearby failure without the matching root error prove nothing", () => {
    const f = fixture();
    expect(
      eligible(
        f,
        f.events.filter((event) => event.sequence !== 40),
      ),
    ).toEqual([]);
  });
  it("refuses a stale completion identity even when its timestamp and payload match", () => {
    const f = fixture();
    const completion = f.events.at(-1)!;
    expect(
      eligible(f, f.events, f.receipts, {
        ...completion,
        event: { ...completion.event, id: EventId.make("stale") },
      }),
    ).toEqual([]);
    expect(eligible(f, f.events, f.receipts, { ...completion, sequence: 9 })).toEqual([]);
  });
  it("later bookkeeping on an earlier execution cannot demonstrate recovery", () => {
    const f = fixture();
    const earlier = run("recovery", 1, "completed");
    const events = [
      f.events[0]!,
      update(1.5, earlier, "run.created"),
      ...f.events.slice(1).filter((event) => event.sequence !== 70),
    ];
    expect(eligible({ ...f, recovery: earlier }, events)).toEqual([]);
  });
  it("retains a hold after its source attempt or current queued identity changes", () => {
    const f = fixture();
    const events = [
      ...f.events.slice(0, 6),
      update(6.5, { ...f.failed, activeAttemptId: RunAttemptId.make("replacement") }),
      ...f.events.slice(6),
    ];
    expect(eligible(f, events)).toEqual([]);
    expect([
      ...classifyAutomaticQueueHolds({
        events: f.events,
        receipts: f.receipts,
        candidates: [{ ...f.held, activeAttemptId: RunAttemptId.make("replacement") }],
        recovery: { type: "completion", stored: f.events.at(-1)!, current: f.recovery },
      }),
    ]).toEqual([]);
  });
  it("explicit preparation retry recovers only its proved cohort alongside older unknown holds", () => {
    const f = fixture(true);
    const old = { ...run("older", 1, "queued"), queueHeld: true };
    const events = [f.events[0]!, update(1.5, old, "run.created"), ...f.events.slice(1, 6)];
    expect([
      ...classifyAutomaticQueueHolds({
        events,
        receipts: f.receipts,
        candidates: [old, f.held],
        recovery: { type: "preparation", current: f.failed },
      }),
    ]).toEqual([f.held.id]);
    expect([
      ...classifyAutomaticQueueHolds({
        events,
        receipts: [
          ...f.receipts,
          {
            command_id: CommandId.make("stop"),
            command_type: "run.interrupt",
            result_sequence: 60,
          },
        ],
        candidates: [old, f.held],
        recovery: { type: "preparation", current: f.failed },
      }),
    ]).toEqual([]);
  });
  it("retains holds with truncated history or ambiguous zero-sequence Stop receipts", () => {
    const f = fixture();
    expect(eligible(f, f.events.slice(1))).toEqual([]);
    expect(
      eligible(f, f.events, [
        ...f.receipts,
        {
          command_id: CommandId.make("unknown-stop"),
          command_type: "thread.stop",
          result_sequence: 0,
        },
      ]),
    ).toEqual([]);
  });
});
