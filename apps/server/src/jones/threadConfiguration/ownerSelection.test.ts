import { describe, expect, it } from "@effect/vitest";
import {
  MessageId,
  NodeId,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  RunAttemptId,
  RunId,
  ThreadId,
  type ModelSelection,
  type OrchestrationV2Run,
  type OrchestrationV2RunAttempt,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import {
  admittedSessionIds,
  isOwnerSelectionExplicit,
  isOwnerSelectionMessage,
  ownerPromoteSelection,
  ownerSelectionCommit,
  ownerSteerSelection,
  pendingOwnerSelection,
  rebindPreAdmissionRun,
} from "./ownerSelection.ts";

const a: ModelSelection = { instanceId: ProviderInstanceId.make("a"), model: "model" };
const c: ModelSelection = {
  instanceId: ProviderInstanceId.make("c"),
  model: "new-model",
  options: [
    { id: "effort", value: "max" },
    { id: "fast", value: true },
  ],
};
const now = DateTime.makeUnsafe("2026-10-26T00:00:00Z");
const run: OrchestrationV2Run = {
  id: RunId.make("run"),
  threadId: ThreadId.make("thread"),
  ordinal: 2,
  providerInstanceId: a.instanceId,
  modelSelection: a,
  providerThreadId: ProviderThreadId.make("provider"),
  userMessageId: MessageId.make("message"),
  rootNodeId: NodeId.make("root"),
  activeAttemptId: RunAttemptId.make("attempt"),
  status: "queued",
  queuePosition: 3,
  queueHeld: true,
  requestedAt: now,
  startedAt: null,
  completedAt: null,
  checkpointId: null,
  contextHandoffId: null,
};
const attempt: OrchestrationV2RunAttempt = {
  id: run.activeAttemptId!,
  runId: run.id,
  rootNodeId: run.rootNodeId!,
  attemptOrdinal: 1,
  reason: "initial",
  providerInstanceId: a.instanceId,
  providerThreadId: run.providerThreadId!,
  providerTurnId: null,
  status: "pending",
  startedAt: null,
  completedAt: null,
};
const event = { type: "thread.model-selection-updated", payload: { modelSelection: c } };
const message = { type: "message.dispatch", createdBy: "user", modelSelection: c };

describe("owner selection authority and ordering", () => {
  it("decide and commit classifiers agree, including equal and legacy picks", () => {
    for (const ingress of [false, true]) {
      for (const type of ["message.dispatch", "thread.model-selection.set", "provider.switch"]) {
        for (const explicit of [undefined, false, true]) {
          for (const saved of [a, c]) {
            const command = { ...message, type, modelSelectionExplicit: explicit };
            const decided = isOwnerSelectionExplicit(command, ingress, saved);
            expect(
              ownerSelectionCommit(command, ingress, decided ? [event] : []) !== undefined,
            ).toBe(decided);
          }
        }
      }
    }
    expect(isOwnerSelectionExplicit({ ...message, modelSelectionExplicit: true }, true, c)).toBe(
      true,
    );
    expect(isOwnerSelectionExplicit(message, true, c)).toBe(false);
  });

  it("wire user authors, agent setters, recovery and automatic delivery confer no authority", () => {
    expect(isOwnerSelectionExplicit(message, false, a)).toBe(false);
    for (const extra of [
      { createdBy: "agent" },
      { restartContinuationOfRunId: "source" },
      { manualContinuationOfRunId: "source" },
      { usageLimitContinuationOfRunId: "source" },
      { notification: {} },
      { delegatedCompletion: {} },
    ]) {
      const command = { ...message, modelSelectionExplicit: true, ...extra };
      expect(isOwnerSelectionMessage(command, true)).toBe(false);
      expect(isOwnerSelectionExplicit(command, true, a)).toBe(false);
      expect(ownerSelectionCommit(command, true, [event])).toBeUndefined();
    }
  });

  it("pending authority requires the complete saved tuple to still match", () => {
    const saved = { ...c, options: [...c.options!].reverse() };
    expect(pendingOwnerSelection({ modelSelection: c }, saved)).toEqual(saved);
    expect(pendingOwnerSelection({ modelSelection: c }, { ...c, options: [] })).toBeUndefined();
    expect(pendingOwnerSelection(null, c)).toBeUndefined();
  });

  it("rebinding changes only pre-admission selection and attempt instance", () => {
    for (const status of ["preparing", "queued"] as const) {
      const changed = rebindPreAdmissionRun({ ...run, status }, attempt, c, now);
      expect(changed.map((event) => event.type)).toEqual(["run.updated", "run-attempt.updated"]);
      expect(changed[0]?.payload).toEqual({
        ...run,
        status,
        modelSelection: c,
        providerInstanceId: c.instanceId,
      });
      expect(changed[1]?.payload).toEqual({ ...attempt, providerInstanceId: c.instanceId });
    }
    for (const status of [
      "starting",
      "running",
      "waiting",
      "completed",
      "interrupted",
      "cancelled",
      "failed",
    ] as const)
      expect(rebindPreAdmissionRun({ ...run, status }, attempt, c, now)).toEqual([]);
    expect(rebindPreAdmissionRun(run, attempt, a, now)).toEqual([]);
    const continuation = { ...run, restartContinuationOfRunId: RunId.make("crashed-source") };
    expect(rebindPreAdmissionRun(continuation, attempt, c, now)[0]?.payload).toEqual({
      ...continuation,
      modelSelection: c,
      providerInstanceId: c.instanceId,
    });
  });

  it("implicit owner sends and single promotion respect pending C, fresh agent saves stay prospective", () => {
    expect(
      ownerSteerSelection({
        ownerMessage: true,
        explicit: false,
        pending: c,
        requested: a,
        active: a,
      }),
    ).toEqual({ modelSelection: c, ownerTransition: true });
    expect(
      ownerSteerSelection({
        ownerMessage: true,
        explicit: false,
        pending: undefined,
        requested: c,
        active: a,
      }),
    ).toEqual({ modelSelection: a, ownerTransition: false });
    expect(
      ownerSteerSelection({
        ownerMessage: false,
        explicit: true,
        pending: c,
        requested: c,
        active: a,
      }),
    ).toEqual({ modelSelection: a, ownerTransition: false });
    expect(ownerPromoteSelection({ ingress: true, pending: c, queued: c, active: a })).toEqual({
      modelSelection: c,
      ownerTransition: true,
    });
    expect(
      ownerPromoteSelection({ ingress: true, pending: undefined, queued: c, active: a }),
    ).toEqual({ modelSelection: a, ownerTransition: false });
    expect(
      ownerPromoteSelection({ ingress: true, pending: c, queued: a, active: a }).ownerTransition,
    ).toBe(false);
    expect(
      ownerPromoteSelection({
        ingress: false,
        automaticBoundary: true,
        pending: c,
        queued: c,
        active: a,
      }),
    ).toEqual({ modelSelection: c, ownerTransition: false });
  });

  it("starting, running and waiting bindings are excluded from setter detach", () => {
    const session = ProviderSessionId.make("session");
    // Only the two fields used by the policy are needed for this synthetic binding.
    const threads = [{ id: run.providerThreadId!, providerSessionId: session }];
    for (const status of ["starting", "running", "waiting"] as const)
      expect(admittedSessionIds([{ ...run, status }], threads)).toEqual(new Set([session]));
    expect(admittedSessionIds([run], threads)).toEqual(new Set());
  });
});
