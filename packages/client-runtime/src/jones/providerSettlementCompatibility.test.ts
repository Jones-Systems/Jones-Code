import {
  NodeId,
  MessageId,
  ProviderThreadId,
  RunId,
  RunAttemptId,
  ProviderTurnId,
  type OrchestrationV2RunStatus,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { describe, expect, it } from "vite-plus/test";
import { v2Projection } from "../state/orchestrationV2TestFixtures.ts";
import {
  deriveLatestThreadRun,
  deriveThreadActivityRun,
  deriveThreadRuntime,
  threadRuntimeHasInterruptibleRun,
} from "../state/threadExecution.ts";

const now = DateTime.makeUnsafe("2026-07-28T10:00:00.000Z");

function run(id: string, ordinal: number, status: OrchestrationV2RunStatus) {
  return {
    id: RunId.make(id),
    threadId: v2Projection.thread.id,
    ordinal,
    providerInstanceId: v2Projection.thread.providerInstanceId,
    modelSelection: v2Projection.thread.modelSelection,
    providerThreadId: null,
    userMessageId: MessageId.make(`message-${id}`),
    rootNodeId: null,
    activeAttemptId: null,
    status,
    requestedAt: now,
    startedAt: status === "queued" ? null : now,
    completedAt: null,
    checkpointId: null,
    contextHandoffId: null,
  };
}

describe("provider response settlement and operational activity", () => {
  it.each([undefined, null, "completed", "interrupted", "failed", "cancelled"] as const)(
    "keeps %s provider evidence independent from checkpoint-wait and Stop",
    (outcome) => {
      const attemptId = RunAttemptId.make("attempt:presentation");
      const providerTurnId = ProviderTurnId.make("provider-turn:presentation");
      const completedAt = DateTime.add(now, { seconds: 5 });
      const current = { ...run("run-presentation", 1, "waiting"), activeAttemptId: attemptId };
      const providerSettlement =
        outcome === undefined
          ? undefined
          : outcome === null
            ? null
            : {
                runAttemptId: attemptId,
                providerTurnId,
                status: outcome,
                completedAt,
              };
      const attempt = {
        id: attemptId,
        runId: current.id,
        attemptOrdinal: 1,
        rootNodeId: NodeId.make("node:presentation"),
        providerInstanceId: current.providerInstanceId,
        providerThreadId: ProviderThreadId.make("provider-thread:presentation"),
        providerTurnId,
        reason: "initial" as const,
        status: "completed" as const,
        startedAt: now,
        completedAt,
        ...(outcome === undefined ? {} : { providerSettlement }),
      };
      const projection = { ...v2Projection, runs: [current], attempts: [attempt] };
      const summary = deriveLatestThreadRun(projection);
      expect(summary?.status).toBe("waiting");
      expect(summary?.completedAt).toBeNull();
      expect(summary?.providerSettlement).toEqual(
        providerSettlement == null
          ? providerSettlement
          : { ...providerSettlement, completedAt: DateTime.formatIso(completedAt) },
      );
      expect(Object.hasOwn(summary!, "providerSettlement")).toBe(outcome !== undefined);
      expect(deriveThreadActivityRun(projection)).toEqual(summary);
      expect(threadRuntimeHasInterruptibleRun(deriveThreadRuntime(projection))).toBe(false);
      const afterCheckpoint = {
        ...projection,
        runs: [
          {
            ...current,
            status: "completed" as const,
            completedAt: DateTime.add(completedAt, { minutes: 1 }),
          },
        ],
      };
      expect(deriveLatestThreadRun(afterCheckpoint)?.providerSettlement).toEqual(
        summary?.providerSettlement,
      );
      const newer = {
        ...run("newer", 2, "running"),
        activeAttemptId: RunAttemptId.make("newer-attempt"),
      };
      expect(
        deriveLatestThreadRun({ ...projection, runs: [current, newer] })?.providerSettlement,
      ).toBeUndefined();
    },
  );
});
