import { describe, expect, it } from "vite-plus/test";
import {
  NodeId,
  ContextTransferId,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  RuntimeRequestId,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import {
  assertCurrentBinding,
  continuationRoot,
  idleEligibility,
  sourceBinding,
  TaskContinuationError,
} from "./sourceBinding.ts";
import { requestIdentity } from "./TaskContinuation.ts";
import { fixture } from "./testkit.ts";

const isError = Schema.is(TaskContinuationError);
describe("source task continuation binding", () => {
  it("resolves a production-shaped untagged original through its consumed spawn and accepted attempt", () => {
    const f = fixture();
    expect(f.run.delegatedTaskId).toBeUndefined();
    expect(f.task.providerThreadId).toBeNull();
    const binding = sourceBinding(f.task, f.child);
    expect(binding).toMatchObject({
      sourceRunId: f.run.id,
      sourceAttemptId: f.attempt.id,
      nativeSessionId: "original-native",
      providerThreadId: f.provider.id,
    });
    if (isError(binding)) throw binding;
    expect(assertCurrentBinding(binding, f.child)).toBeUndefined();
    expect(idleEligibility(f.parent, f.child)).toBeUndefined();
  });
  it("does not substitute today's provider row for a legacy or unaccepted attempt", () => {
    const f = fixture();
    const { nativeThreadId: _native, ...legacy } = f.attempt;
    expect(sourceBinding(f.task, { ...f.child, attempts: [legacy] })).toMatchObject({
      reason: "native_source_uncaptured",
    });
    expect(sourceBinding(f.task, { ...f.child, providerTurns: [] })).toMatchObject({
      reason: "native_source_unavailable",
    });
  });
  it("refuses missing, ambiguous or conflicting spawn links instead of guessing a child run", () => {
    const f = fixture();
    const spawn = f.child.contextTransfers[0]!;
    for (const contextTransfers of [
      [],
      [spawn, { ...spawn, id: ContextTransferId.make("duplicate-spawn") }],
      [{ ...spawn, status: "pending" as const }],
      [{ ...spawn, targetRunId: RunId.make("missing-run") }],
    ])
      expect(sourceBinding(f.task, { ...f.child, contextTransfers })).toMatchObject({
        reason: "source_link_unavailable",
      });
    expect(
      sourceBinding(f.task, { ...f.child, runs: [{ ...f.run, delegatedTaskId: f.task.id }] }),
    ).toMatchObject({ reason: "native_source_conflict" });
    expect(
      sourceBinding({ ...f.task, id: NodeId.make("unbound-followup") }, f.child),
    ).toMatchObject({
      reason: "source_link_unavailable",
    });
  });
  it("never substitutes a later untagged wake's native conversation for the original", () => {
    const f = fixture();
    const wake = { ...f.run, id: RunId.make("wake"), ordinal: 2 };
    const wakeAttempt = {
      ...f.attempt,
      id: RunAttemptId.make("wake-attempt"),
      runId: wake.id,
      providerTurnId: ProviderTurnId.make("wake-turn"),
      nativeThreadId: "replacement-native",
    };
    const child = {
      ...f.child,
      runs: [f.run, wake],
      attempts: [f.attempt, wakeAttempt],
      providerTurns: [
        ...f.child.providerTurns,
        {
          ...f.child.providerTurns[0]!,
          id: wakeAttempt.providerTurnId,
          runAttemptId: wakeAttempt.id,
        },
      ],
    };
    const binding = sourceBinding(f.task, child);
    if (isError(binding)) throw binding;
    expect(binding.nativeSessionId).toBe("original-native");
    expect(binding.sourceRunId).toBe(f.run.id);
    expect(
      sourceBinding(f.task, {
        ...child,
        attempts: [f.attempt, { ...wakeAttempt, nativeThreadId: "original-native" }],
      }),
    ).toMatchObject({ nativeSessionId: "original-native", sourceRunId: f.run.id });
    expect(assertCurrentBinding(binding, child)).toBeUndefined();
    expect(
      assertCurrentBinding(binding, {
        ...child,
        providerThreads: [
          {
            ...f.provider,
            nativeThreadRef: { ...f.provider.nativeThreadRef!, nativeId: "replacement-native" },
          },
        ],
      }),
    ).toMatchObject({ reason: "native_reference_moved" });
  });
  it("accepts restart outcomes only when every accepted chain attempt preserves the native identity", () => {
    const f = fixture();
    const successor = {
      ...f.run,
      id: RunId.make("restart"),
      ordinal: 2,
      restartContinuationOfRunId: f.run.id,
    };
    const attempt = { ...f.attempt, id: RunAttemptId.make("restart-attempt"), runId: successor.id };
    const child = {
      ...f.child,
      runs: [f.run, successor],
      attempts: [f.attempt, attempt],
      providerTurns: [
        ...f.child.providerTurns,
        {
          ...f.child.providerTurns[0]!,
          id: ProviderTurnId.make("restart-turn"),
          runAttemptId: attempt.id,
        },
      ],
    };
    // Each attempt names its own turn; contradictory cross-attempt links must refuse.
    const accepted = {
      ...child,
      attempts: [f.attempt, { ...attempt, providerTurnId: child.providerTurns[1]!.id }],
    };
    expect(sourceBinding(f.task, accepted)).toMatchObject({
      sourceRunId: successor.id,
      sourceAttemptId: attempt.id,
      nativeSessionId: "original-native",
    });
    expect(
      sourceBinding(f.task, {
        ...accepted,
        attempts: [f.attempt, { ...accepted.attempts[1]!, nativeThreadId: "other-native" }],
      }),
    ).toMatchObject({ reason: "native_source_conflict" });
    expect(
      sourceBinding(f.task, {
        ...accepted,
        attempts: [{ ...f.attempt, nativeThreadId: "" }, accepted.attempts[1]!],
      }),
    ).toMatchObject({ reason: "native_source_uncaptured" });
    for (const changed of [
      { ...successor, providerInstanceId: ProviderInstanceId.make("other-account") },
      { ...successor, providerThreadId: ProviderThreadId.make("other-row") },
      { ...successor, modelSelection: { ...successor.modelSelection, options: [] } },
    ])
      expect(sourceBinding(f.task, { ...accepted, runs: [f.run, changed] })).toMatchObject({
        reason: "native_source_conflict",
      });
    const followup = { ...f.task, id: NodeId.make("followup") };
    expect(
      sourceBinding(followup, {
        ...accepted,
        runs: accepted.runs.map((run) => ({ ...run, delegatedTaskId: followup.id })),
      }),
    ).toMatchObject({ sourceRunId: successor.id, nativeSessionId: "original-native" });
  });
  it("accepts older exact-turn links but gives contradictory attribution precedence", () => {
    const f = fixture();
    const turn = f.child.providerTurns[0]!;
    const legacy = { ...f.child, providerTurns: [{ ...turn, runAttemptId: null }] };
    expect(sourceBinding(f.task, legacy)).toMatchObject({ nativeSessionId: "original-native" });
    const settlement = {
      runAttemptId: f.attempt.id,
      providerTurnId: turn.id,
      status: "completed" as const,
      completedAt: f.now,
    };
    expect(
      sourceBinding(f.task, {
        ...legacy,
        attempts: [{ ...f.attempt, providerTurnId: null, providerSettlement: settlement }],
      }),
    ).toMatchObject({ nativeSessionId: "original-native" });
    for (const child of [
      {
        ...f.child,
        providerTurns: [{ ...turn, providerThreadId: ProviderThreadId.make("other-row") }],
      },
      {
        ...f.child,
        providerTurns: [{ ...turn, runAttemptId: RunAttemptId.make("other-attempt") }],
      },
      {
        ...f.child,
        attempts: [
          {
            ...f.attempt,
            providerSettlement: { ...settlement, runAttemptId: RunAttemptId.make("other-attempt") },
          },
        ],
      },
      {
        ...f.child,
        attempts: [{ ...f.attempt, providerSettlement: settlement }],
        providerTurns: [{ ...turn, runAttemptId: RunAttemptId.make("other-attempt") }],
      },
    ])
      expect(sourceBinding(f.task, child)).toMatchObject({ reason: "native_source_conflict" });
    const uncaptured = sourceBinding(f.task, {
      ...f.child,
      attempts: [{ ...f.attempt, nativeThreadId: "" }],
    });
    expect(uncaptured).toMatchObject({ reason: "native_source_uncaptured" });
    if (!isError(uncaptured)) throw new Error("expected missing-capture refusal");
    expect(uncaptured.message).toContain("no captured Claude session ID");
    expect(uncaptured.message).toContain("cannot recover");
  });
  it("rejects a moved native ref, selection, unpublished source, and remote child", () => {
    const f = fixture();
    const binding = sourceBinding(f.task, f.child);
    if (isError(binding)) throw binding;
    expect(
      assertCurrentBinding(binding, {
        ...f.child,
        providerThreads: [
          {
            ...f.provider,
            nativeThreadRef: { ...f.provider.nativeThreadRef!, nativeId: "fresh-native" },
          },
        ],
      }),
    ).toMatchObject({ reason: "native_reference_moved" });
    expect(
      assertCurrentBinding(binding, {
        ...f.child,
        thread: {
          ...f.child.thread,
          modelSelection: {
            ...f.child.thread.modelSelection,
            options: [{ id: "effort", value: "high" }],
          },
        },
      }),
    ).toMatchObject({ reason: "selection_changed" });
    expect(sourceBinding({ ...f.task, result: null }, f.child)).toMatchObject({
      reason: "source_not_completed",
    });
    expect(sourceBinding({ ...f.task, childThreadId: null }, f.child)).toMatchObject({
      reason: "task_not_found",
    });
  });
  it("blocks held queues, pending input, secret cards and live nested/background ownership", () => {
    const f = fixture();
    const pendingSecret = {
      type: "secret_request",
      secretStatus: "pending",
    } as OrchestrationV2TurnItem;
    const pendingRequest: OrchestrationV2RuntimeRequest = {
      id: RuntimeRequestId.make("pending-request"),
      nodeId: f.attempt.rootNodeId,
      providerTurnId: f.attempt.providerTurnId,
      nativeRequestRef: null,
      kind: "user_input",
      status: "pending",
      responseCapability: { type: "message" },
      createdAt: f.now,
      resolvedAt: null,
    };
    for (const child of [
      {
        ...f.child,
        runs: [
          ...f.child.runs,
          { ...f.run, id: RunId.make("held"), status: "queued" as const, queueHeld: true },
        ],
      },
      { ...f.child, runtimeRequests: [pendingRequest] },
      { ...f.child, turnItems: [pendingSecret] },
      {
        ...f.child,
        subagents: [
          { ...f.task, id: NodeId.make("nested"), status: "running" as const, result: null },
        ],
      },
      {
        ...f.child,
        providerThreads: [
          {
            ...f.provider,
            pendingBackgroundTasks: [
              { kind: "background_task" as const, taskId: "running-background" },
            ],
          },
        ],
      },
    ])
      expect(idleEligibility(f.parent, child)).toMatchObject({ reason: "child_busy" });
    expect(
      idleEligibility(f.parent, {
        ...f.child,
        subagents: [{ ...f.task, id: NodeId.make("finished-nested"), result: null }],
      }),
    ).toBeUndefined();
  });
  it("walks bounded same-task restart ancestry and rejects cycles or moved selection", () => {
    const f = fixture();
    const successor = {
      ...f.run,
      id: RunId.make("successor"),
      restartContinuationOfRunId: f.run.id,
    };
    expect(continuationRoot([f.run, successor], successor)).toEqual(f.run);
    expect(continuationRoot([successor], successor)).toMatchObject({ reason: "ancestry_invalid" });
    expect(
      continuationRoot(
        [{ ...f.run, restartContinuationOfRunId: successor.id }, successor],
        successor,
      ),
    ).toMatchObject({ reason: "ancestry_invalid" });
    expect(
      continuationRoot([f.run, successor], {
        ...successor,
        delegatedTaskId: NodeId.make("another-task"),
      }),
    ).toMatchObject({ reason: "ancestry_invalid" });
    expect(
      continuationRoot([f.run, successor], {
        ...successor,
        providerThreadId: ProviderThreadId.make("moved-row"),
      }),
    ).toMatchObject({ reason: "ancestry_invalid" });
  });
  it("derives stable caller-scoped keys and a digest covering every decoded request field", () => {
    const f = fixture();
    const input = { taskId: f.task.id, message: "Next", clientRequestId: "round2" };
    const initial = requestIdentity(f.parent.thread.id, input);
    expect(requestIdentity(f.parent.thread.id, { ...input })).toEqual(initial);
    expect(requestIdentity(f.parent.thread.id, { ...input, message: "Changed" }).commandId).toBe(
      initial.commandId,
    );
    for (const changed of [
      { ...input, message: "Changed" },
      { ...input, taskId: NodeId.make("other") },
      { ...input, expectedNativeSessionId: "original-native" },
    ])
      expect(requestIdentity(f.parent.thread.id, changed).requestDigest).not.toBe(
        initial.requestDigest,
      );
  });
});
