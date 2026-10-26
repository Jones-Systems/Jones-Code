import { describe, expect, it } from "vite-plus/test";
import {
  NodeId,
  MessageId,
  ProviderThreadId,
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
  it("binds the legacy initial app-owned run without delegatedTaskId or a spawn transfer", () => {
    const f = fixture(true);
    expect(f.run.delegatedTaskId).toBeUndefined();
    expect(f.child.contextTransfers).toEqual([]);
    expect(sourceBinding(f.task, f.child)).toMatchObject({
      sourceTaskId: f.task.id,
      sourceRunId: f.run.id,
      sourceAttemptId: f.attempt.id,
      nativeSessionId: "original-native",
    });
  });
  it("uses a matching spawn transfer and follows only the legacy run's restart ancestry", () => {
    const f = fixture(true);
    const child = {
      ...f.child,
      runs: [{ ...f.run, userMessageId: MessageId.make("historical-message") }],
      contextTransfers: [f.spawnTransfer],
    };
    expect(sourceBinding(f.task, child)).toMatchObject({ nativeSessionId: "original-native" });
    const successor = {
      ...f.run,
      id: RunId.make("legacy-successor"),
      ordinal: 2,
      restartContinuationOfRunId: f.run.id,
    };
    const unrelated = {
      ...f.run,
      id: RunId.make("unrelated"),
      ordinal: 3,
      userMessageId: MessageId.make("unrelated-message"),
    };
    expect(
      sourceBinding(f.task, {
        ...f.child,
        runs: [f.run, successor, unrelated],
        attempts: [{ ...f.attempt, runId: successor.id }],
      }),
    ).toMatchObject({ sourceRunId: successor.id, nativeSessionId: "original-native" });
  });
  it("rejects foreign, ambiguous or explicitly mismatched legacy lineage", () => {
    const f = fixture(true);
    for (const child of [
      {
        ...f.child,
        thread: {
          ...f.child.thread,
          lineage: { ...f.child.thread.lineage, parentThreadId: f.child.thread.id },
        },
      },
      {
        ...f.child,
        thread: {
          ...f.child.thread,
          forkedFrom: { type: "node" as const, nodeId: NodeId.make("foreign-task") },
        },
      },
      { ...f.child, runs: [{ ...f.run, userMessageId: MessageId.make("foreign-message") }] },
      { ...f.child, runs: [{ ...f.run, delegatedTaskId: NodeId.make("foreign-task") }] },
      { ...f.child, runs: [f.run, { ...f.run, id: RunId.make("ambiguous-initial") }] },
      { ...f.child, runs: [{ ...f.run, ordinal: 2 }] },
      { ...f.child, contextTransfers: [f.spawnTransfer, f.spawnTransfer] },
      {
        ...f.child,
        contextTransfers: [{ ...f.spawnTransfer, targetRunId: RunId.make("foreign-run") }],
      },
      {
        ...f.child,
        contextTransfers: [
          {
            ...f.spawnTransfer,
            sourcePoint: {
              ...f.spawnTransfer.sourcePoint,
              runId: RunId.make("foreign-parent-run"),
            },
          },
        ],
      },
      { ...f.child, providerTurns: [] },
      { ...f.child, attempts: [{ ...f.attempt, status: "failed" as const }] },
      {
        ...f.child,
        attempts: [{ ...f.attempt, providerInstanceId: f.parent.thread.providerInstanceId }],
      },
    ])
      expect(sourceBinding(f.task, child)).toMatchObject({ reason: "native_source_unavailable" });
    expect(sourceBinding({ ...f.task, id: NodeId.make("foreign-task") }, f.child)).toMatchObject({
      reason: "native_source_unavailable",
    });
    const successor = {
      ...f.run,
      id: RunId.make("relabelled"),
      ordinal: 2,
      restartContinuationOfRunId: f.run.id,
      delegatedTaskId: f.task.id,
    };
    expect(sourceBinding(f.task, { ...f.child, runs: [f.run, successor] })).toMatchObject({
      reason: "native_source_unavailable",
    });
    for (const corrupt of [
      { ...successor, delegatedTaskId: NodeId.make("foreign-task") },
      {
        ...successor,
        delegatedTaskId: undefined,
        providerThreadId: ProviderThreadId.make("moved"),
      },
      {
        ...successor,
        delegatedTaskId: undefined,
        modelSelection: { ...f.run.modelSelection, model: "different-model" },
      },
    ])
      expect(sourceBinding(f.task, { ...f.child, runs: [f.run, corrupt] })).toMatchObject({
        reason: "native_source_unavailable",
      });
  });
  it("uses the completed child's accepted attempt when initial parent task provider metadata is null", () => {
    const f = fixture();
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
  it.each([false, true])(
    "does not substitute today's row for an unaccepted attempt (legacy=%s)",
    (legacy) => {
      const f = fixture(legacy);
      const { nativeThreadId: _native, ...unrecorded } = f.attempt;
      expect(sourceBinding(f.task, { ...f.child, attempts: [unrecorded] })).toMatchObject({
        reason: "native_source_unavailable",
      });
      expect(sourceBinding(f.task, { ...f.child, providerTurns: [] })).toMatchObject({
        reason: "native_source_unavailable",
      });
    },
  );
  it.each([false, true])(
    "rejects moved refs, selection, unpublished source and remote child (legacy=%s)",
    (legacy) => {
      const f = fixture(legacy);
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
    },
  );
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
