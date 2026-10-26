import { describe, expect, it } from "vite-plus/test";
import {
  NodeId,
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
  it("does not substitute today's provider row for a legacy or unaccepted attempt", () => {
    const f = fixture();
    const { nativeThreadId: _native, ...legacy } = f.attempt;
    expect(sourceBinding(f.task, { ...f.child, attempts: [legacy] })).toMatchObject({
      reason: "native_source_unavailable",
    });
    expect(sourceBinding(f.task, { ...f.child, providerTurns: [] })).toMatchObject({
      reason: "native_source_unavailable",
    });
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
